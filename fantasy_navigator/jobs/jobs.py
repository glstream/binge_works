import dagster as dg


# rookie jobs
ktc_rookies_job = dg.define_asset_job(
    name="ktc_rookies_job",
    selection=["extract_one_qb_ktc_rookie_picks","load_one_qb_ktc_rookie_picks","extract_sf_ktc_rookie_picks", "load_sf_ktc_rookie_picks"],
    description="Job to update KTC rookie picks data daily",
    op_retry_policy=dg.RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=dg.Backoff.EXPONENTIAL,
    ),
)

# projections jobs
cbs_projections_job = dg.define_asset_job(
    name="cbs_projections_job",
    selection="cbs_raw_projections_data*",
    description="Job to update CBS projections data",
    op_retry_policy=dg.RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=dg.Backoff.EXPONENTIAL,
    ),
)

espn_projections_job = dg.define_asset_job(
    name="espn_projections_job",
    selection="espn_raw_projections_data*",
    description="Job to update ESPN projections data",
    op_retry_policy=dg.RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=dg.Backoff.EXPONENTIAL,
    ),
)

nfl_projections_job = dg.define_asset_job(
    name="nfl_projections_job",
    selection="nfl_raw_projections_data*",
    description="Job to update nfl projections data",
    op_retry_policy=dg.RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=dg.Backoff.EXPONENTIAL,
    ),
)

fantasy_navigator_job = dg.define_asset_job(
    name="fantasy_navigator_job",
    selection=[
        "extract_one_qb_ktc_rookie_picks",
        "load_one_qb_ktc_rookie_picks",
        "extract_sf_ktc_rookie_picks",
        "load_sf_ktc_rookie_picks",
        #KTC Rankings
        "ktc_raw_player_data",
        "ktc_player_ranks_loaded",
        "ktc_player_ranks_formatted",
        # Fantasy Calc Rankings
        "fc_sf_raw_player_data",
        "fc_player_ranks_sf_loaded",
        "fc_one_qb_raw_player_data",
        "fc_player_ranks_one_qb_loaded",
        "fc_player_ranks_formatted",
        # dynasty process rankings
        "dp_raw_player_data",
        "dp_player_ranks_loaded",
        "dp_player_ranks_formatted",
        # dynasty daddy rankings
        "dd_raw_dynasty_data",
        "dd_player_ranks_dynasty_loaded",
        "dd_raw_redraft_data",
        "dd_player_ranks_redraft_loaded",
        "dd_player_ranks_formatted",
        #Fantasy Navigator Pocessing
        "raw_player_asset_values",
        "processed_player_ranks",
        "load_processed_player_ranks",
        "load_future_draft_picks",
        "fn_player_ranks_formatted"
        
        ],
    description="Job to update Fantasy Navigator rankings data weekly",
    op_retry_policy=dg.RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=dg.Backoff.EXPONENTIAL,
    ),
)

fantasy_navigator_hist_job = dg.define_asset_job(
    name="fantasy_navigator_hist_job",
    selection=[
        "extract_one_qb_ktc_rookie_picks",
        "load_one_qb_ktc_rookie_picks",
        "extract_sf_ktc_rookie_picks",
        "load_sf_ktc_rookie_picks",
        #KTC Rankings
        "ktc_raw_player_data",
        "ktc_player_ranks_loaded",
        "ktc_player_ranks_formatted",
        # Fantasy Calc Rankings
        "fc_sf_raw_player_data",
        "fc_player_ranks_sf_loaded",
        "fc_one_qb_raw_player_data",
        "fc_player_ranks_one_qb_loaded",
        "fc_player_ranks_formatted",
        # dynasty process rankings
        "dp_raw_player_data",
        "dp_player_ranks_loaded",
        "dp_player_ranks_formatted",
        # dynasty daddy rankings
        "dd_raw_dynasty_data",
        "dd_player_ranks_dynasty_loaded",
        "dd_raw_redraft_data",
        "dd_player_ranks_redraft_loaded",
        "dd_player_ranks_formatted",
        #Fantasy Navigator history Pocessing
        "create_sf_ranks_history",
        "truncate_sf_player_ranks_staging",
        "raw_player_asset_values_hist",
        "processed_player_ranks_hist",
        "load_sf_player_ranks_staging",
        "load_sf_player_ranks_hist",
        #Fantasy Navigator current Pocessing
        "raw_player_asset_values",
        "processed_player_ranks",
        "load_processed_player_ranks",
        "load_future_draft_picks",
        "fn_player_ranks_formatted"
        ],
    description="Job to update Fantasy Navigator history data weekly",
    op_retry_policy=dg.RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=dg.Backoff.EXPONENTIAL,
    ),
)
