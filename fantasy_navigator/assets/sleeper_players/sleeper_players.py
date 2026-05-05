import dagster as dg
import requests
from psycopg2.extras import execute_batch

TARGET_TABLE = "dynastr.players"
SLEEPER_PLAYERS_URL = "https://api.sleeper.app/v1/players/nfl"
_OWNERS = ["grayson.stream@gmail.com"]


@dg.asset(
    name="sleeper_raw_player_data",
    description="Fetches all NFL player data from the Sleeper API.",
    compute_kind="python",
    owners=_OWNERS,
    metadata={"source": SLEEPER_PLAYERS_URL},
)
def sleeper_raw_player_data(context: dg.AssetExecutionContext) -> dg.Output[list[dict]]:
    """
    Fetches the full NFL player dataset from Sleeper API and filters
    to active/relevant players (QB, RB, WR, TE).
    """
    context.log.info(f"Fetching player data from {SLEEPER_PLAYERS_URL}")
    try:
        res = requests.get(SLEEPER_PLAYERS_URL, timeout=60)
        res.raise_for_status()
        players_data = res.json()
    except requests.exceptions.RequestException as e:
        context.log.error(f"Failed to fetch Sleeper player data: {e}")
        raise dg.Failure(f"Failed to fetch players from Sleeper API. Error: {e}")

    valid_positions = {"QB", "RB", "WR", "TE"}
    players = []

    for player_id, player in players_data.items():
        position = player.get("position")
        if position not in valid_positions:
            continue

        first_name = player.get("first_name", "")
        last_name = player.get("last_name", "")
        full_name = player.get("full_name", f"{first_name} {last_name}".strip())
        team = player.get("team")
        age = player.get("age")

        if not first_name or not last_name:
            continue

        players.append({
            "player_id": player_id,
            "first_name": first_name,
            "last_name": last_name,
            "full_name": full_name,
            "player_position": position,
            "age": age,
            "team": team,
        })

    context.log.info(f"Parsed {len(players)} players from Sleeper API.")
    return dg.Output(
        value=players,
        metadata={
            "num_players": dg.MetadataValue.int(len(players)),
            "positions": dg.MetadataValue.text(", ".join(valid_positions)),
        },
    )


@dg.asset(
    name="load_sleeper_players",
    description="Loads Sleeper player data into the dynastr.players table.",
    compute_kind="postgres",
    required_resource_keys={"postgres"},
    op_tags={"layer": "final", "sink": "postgres"},
    owners=_OWNERS,
)
def load_sleeper_players(
    context: dg.AssetExecutionContext, sleeper_raw_player_data: list[dict]
) -> dg.Output:
    """
    Upserts player records into dynastr.players using ON CONFLICT
    so existing players get updated and new players get inserted.
    """
    postgres = context.resources.postgres
    players = sleeper_raw_player_data

    upsert_sql = """
    INSERT INTO dynastr.players (player_id, first_name, last_name, full_name, player_position, age, team)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (player_id) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        full_name = EXCLUDED.full_name,
        player_position = EXCLUDED.player_position,
        age = EXCLUDED.age,
        team = EXCLUDED.team;
    """

    data_to_insert = [
        (
            p["player_id"],
            p["first_name"],
            p["last_name"],
            p["full_name"],
            p["player_position"],
            p["age"],
            p["team"],
        )
        for p in players
    ]

    context.log.info(f"Upserting {len(data_to_insert)} players into {TARGET_TABLE}...")

    try:
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                execute_batch(cursor, upsert_sql, data_to_insert, page_size=500)
            conn.commit()
            context.log.info(f"Successfully upserted {len(data_to_insert)} players.")
    except Exception as e:
        context.log.error(f"Error loading player data: {e}")
        raise

    return dg.Output(
        value={"records_upserted": len(data_to_insert)},
        metadata={
            "records_upserted": dg.MetadataValue.int(len(data_to_insert)),
            "target_table": dg.MetadataValue.text(TARGET_TABLE),
        },
    )
