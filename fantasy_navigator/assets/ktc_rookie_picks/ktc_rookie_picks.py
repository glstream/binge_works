import dagster as dg
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from datetime import datetime
import time

TARGET_TABLE = "dynastr.ktc_player_ranks"
_OWNERS = ["grayson.stream@gmail.com"]

@dg.asset(
    name="extract_one_qb_ktc_rookie_picks",
    description="Pulling the rookies from KTC",
    compute_kind="python",
    op_tags={
        "source": "ktc",
        "type": "web-crawler",
    },
    owners=_OWNERS,
)
def extract_one_qb_ktc_rookie_picks(context):
    """Extract the rookie picks from KTC
    This function uses Selenium to scrape the KTC website for rookie picks.
    It uses BeautifulSoup to parse the HTML and extract the relevant data.

    Args:
        context (_type_): _description_

    Returns:
        _type_: _description_
    """    
    one_qb_picks_list = []
    
    for i in range(1,5):
        round = str(i)
        url = f"https://keeptradecut.com/trade-calculator?var=5&pickVal=0&teamOne=2026{round}1|2026{round}2|2026{round}3|2026{round}4|2026{round}5|2026{round}6|2026{round}7|2026{round}8|2026{round}9|2026{round}10|2026{round}11|2026{round}12&teamTwo=&format=1&isStartup=0"
        
        headOption = webdriver.FirefoxOptions()
        headOption.add_argument("--headless")
        
        firefox_service = Service(executable_path='/usr/local/bin/geckodriver')
        browser = webdriver.Firefox(service=firefox_service, options=headOption)

        # browser = webdriver.Firefox(options=headOption)
        browser.get(url)
        time.sleep(3)  # Give page time to render
        html = browser.page_source
        soup = BeautifulSoup(html, features="html.parser")

        rank_type = "dynasty"

        # Use CSS selectors to find pick wrappers
        td = soup.findAll('div', 'team-player-wrapper')
        for k in td:
            player_name_elem = k.find('p', class_='player-name')
            player_value_elem = k.find('div', class_='player-value')

            if player_name_elem and player_value_elem:
                draft_position = player_name_elem.get_text(strip=True)
                draft_pos_value = player_value_elem.get_text(strip=True)

                # Extract round and pick from draft_position (e.g., "2026 Pick 1.01")
                parts = draft_position.split()
                if len(parts) >= 3:
                    year = parts[0]
                    round_pick = parts[2]

                    if '.' in round_pick:
                        round_num, pick_num = round_pick.split('.')
                        draf_pos = f"{year} Round {round_num} Pick {pick_num}"
                        ktc_player_id = round_pick.replace('.', '') + '0002026'
                        entry_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
                        one_qb_picks_list.append([draf_pos, ktc_player_id, draft_pos_value, rank_type, entry_time])

        browser.quit()
    context.log.info(f"one_qb_picks_list: {one_qb_picks_list}")
    
    return dg.Output(
        value=one_qb_picks_list,
        metadata={
            "num_rows": len(one_qb_picks_list),
            "columns": ["draft_position", "player_id", "value", "entry_time"],
        },
        
    )

@dg.asset(
    name="load_one_qb_ktc_rookie_picks",
    description="Load the rookie picks into the database",
    required_resource_keys={"postgres"},
    compute_kind="python",
    owners=_OWNERS,
)
def load_one_qb_ktc_rookie_picks(context, extract_one_qb_ktc_rookie_picks): 
    """
    Load the rookie picks into the database"""
    start_time = time.time()
    
    postgres = context.resources.postgres
    one_qb_picks_list = extract_one_qb_ktc_rookie_picks
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            context.log.info("Inserting obe qb rookie picks into the database")
            insert_query = """
            INSERT INTO dynastr.ktc_player_ranks (
                player_full_name,
                ktc_player_id,
                one_qb_value,
                rank_type,
                insert_date
                )        
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (ktc_player_id, rank_type)
                DO UPDATE SET 
                    one_qb_value = EXCLUDED.one_qb_value,
                    player_full_name = EXCLUDED.player_full_name,
                    insert_date = EXCLUDED.insert_date;
            """
            
            postgres.execute_batch(cursor, insert_query, one_qb_picks_list)
            conn.commit()
            context.log.info(f"Inserted {len(one_qb_picks_list)} rows into the database")
    duration = time.time() - start_time
    return dg.Output(
        value={
            "records_inserted": len(one_qb_picks_list),
            "table_name": "dynastr.ktc_player_ranks",
        },
        metadata={
            "execution_time": dg.MetadataValue.float(duration),
            "records_inserted": dg.MetadataValue.int(len(one_qb_picks_list))      
            },
    )
        
@dg.asset(
    name="extract_sf_ktc_rookie_picks",
    description="superflex rookie picks",
    compute_kind="python",
    owners=_OWNERS,
)
def extract_sf_ktc_rookie_picks(context):
    """Extract the rookie picks from KTC
    This function uses Selenium to scrape the KTC website for rookie picks.
    It uses BeautifulSoup to parse the HTML and extract the relevant data.

    Args:
        context (_type_): _description_

    Returns:
        _type_: _description_
    """    
    sf_picks_list = []
    
    for i in range(1,5):
        round = str(i)
        url = f"https://keeptradecut.com/trade-calculator?var=5&pickVal=0&teamOne=2026{round}1|2026{round}2|2026{round}3|2026{round}4|2026{round}5|2026{round}6|2026{round}7|2026{round}8|2026{round}9|2026{round}10|2026{round}11|2026{round}12&teamTwo=&format=2&isStartup=0"
        
        headOption = webdriver.FirefoxOptions()
        headOption.add_argument("--headless")
        
        firefox_service = Service(executable_path='/usr/local/bin/geckodriver')
        browser = webdriver.Firefox(service=firefox_service, options=headOption)

        browser.get(url)
        time.sleep(3)  # Give page time to render
        html = browser.page_source
        soup = BeautifulSoup(html, features="html.parser")

        rank_type = "dynasty"

        # Use CSS selectors to find pick wrappers
        td = soup.findAll('div', 'team-player-wrapper')
        for k in td:
            player_name_elem = k.find('p', class_='player-name')
            player_value_elem = k.find('div', class_='player-value')

            if player_name_elem and player_value_elem:
                draft_position = player_name_elem.get_text(strip=True)
                draft_pos_value = player_value_elem.get_text(strip=True)

                # Extract round and pick from draft_position (e.g., "2026 Pick 1.01")
                parts = draft_position.split()
                if len(parts) >= 3:
                    year = parts[0]
                    round_pick = parts[2]

                    if '.' in round_pick:
                        round_num, pick_num = round_pick.split('.')
                        draf_pos = f"{year} Round {round_num} Pick {pick_num}"
                        ktc_player_id = round_pick.replace('.', '') + '0002026'
                        entry_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
                        sf_picks_list.append([draf_pos, ktc_player_id, draft_pos_value, rank_type, entry_time])

        browser.quit()
    context.log.info(f"sf_picks_list: {sf_picks_list}")
    
    return dg.Output(
        value=sf_picks_list,
        metadata={
            "num_rows": len(sf_picks_list),
            "columns": ["draft_position", "player_id", "value", "entry_time"],
        },
        
    )
    
@dg.asset(
    name="load_sf_ktc_rookie_picks",
    description="Load the rookie picks into the database",
    required_resource_keys={"postgres"},
    compute_kind="python",
    owners=_OWNERS,
)
def load_sf_ktc_rookie_picks(context, extract_sf_ktc_rookie_picks):
    """
    Load the rookie picks into the database"""
    start_time = time.time()
    
    postgres = context.resources.postgres
    sf_picks_list = extract_sf_ktc_rookie_picks
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            context.log.info("Inserting sf rookie picks into the database")
            insert_query = """
            INSERT INTO dynastr.ktc_player_ranks (
                player_full_name,
                ktc_player_id,
                sf_value,
                rank_type,
                insert_date
                )        
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (ktc_player_id, rank_type)
                DO UPDATE SET 
                    sf_value = EXCLUDED.sf_value,
                    player_full_name = EXCLUDED.player_full_name,
                    insert_date = EXCLUDED.insert_date;
            """
            
            postgres.execute_batch(cursor, insert_query, sf_picks_list)
            conn.commit()
            context.log.info(f"Inserted {len(sf_picks_list)} rows into the database")
    duration = time.time() - start_time
    return dg.Output(
        value={
            "records_inserted": len(sf_picks_list),
            "table_name": "dynastr.ktc_player_ranks",
        },
        metadata={
            "execution_time": dg.MetadataValue.float(duration),
            "records_inserted": dg.MetadataValue.int(len(sf_picks_list))
            },
    )
