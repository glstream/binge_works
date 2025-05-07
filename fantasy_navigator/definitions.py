from dagster import Definitions, load_assets_from_package_module

from binge_works.resources.PostgreSQLResource import PostgreSQLResource


from .constants import (
    postgres_connection_string,
    #RANKINGS
    KTC_ROOKIE_PICKS,
    FN_RANKINGS,
    KTC_RANKINGS,
    FC_RANKINGS,
    DP_RANKINGS,
    DD_RANKINGS,
    FN_RANKINGS_HISTORY,
    #Projections
    CBS_PROJECTIONS,
    ESPN_PROJECTIONS,
    NFL_PROJECTIONS,

)
from .assets import (
    ktc_rookie_picks,
    fn_rankings,
    ktc_rankings,
    fc_rankings,
    dp_rankings,
    dd_rankings,
    fn_rankings_history,
    # projection sources
    cbs_projections,
    espn_projections,
    nfl_projections,
    
    )

from .jobs.jobs import (
    ktc_rookies_job,
    fantasy_navigator_job,
    fantasy_navigator_hist_job,
    # projections
    cbs_projections_job,
    espn_projections_job,
    nfl_projections_job,
    
)
from .schedules.schedules import (
    ktc_rookies_schedule,
    fantasy_navigator_schedule,
    fantasy_navigator_hist_schedule,
    cbs_projections_schedule,
    espn_projections_schedule,
    nfl_projections_schedule,
)

# rookie assets
ktc_rookie_picks_assets = load_assets_from_package_module(ktc_rookie_picks, group_name=KTC_ROOKIE_PICKS)

# rankings assets
fn_rankings_assets = load_assets_from_package_module(fn_rankings, group_name=FN_RANKINGS)
ktc_rankings_assets = load_assets_from_package_module(ktc_rankings, group_name=KTC_RANKINGS)
fc_rankings_assets = load_assets_from_package_module(fc_rankings, group_name=FC_RANKINGS)
dp_rankings_assets = load_assets_from_package_module(dp_rankings, group_name=DP_RANKINGS)
dd_rankings_assets = load_assets_from_package_module(dd_rankings, group_name=DD_RANKINGS)
fn_rankings_history_assets = load_assets_from_package_module(fn_rankings_history, group_name=FN_RANKINGS_HISTORY)   

# projections assets
cbs_projections_assets = load_assets_from_package_module(cbs_projections, group_name=CBS_PROJECTIONS)
espn_projections_assets = load_assets_from_package_module(espn_projections, group_name=ESPN_PROJECTIONS)
nfl_projections_assets = load_assets_from_package_module(nfl_projections, group_name=NFL_PROJECTIONS)

# Load assets from modules
asset_defs = [
    # rankings assets
    *ktc_rookie_picks_assets,
    *fn_rankings_assets,
    *ktc_rankings_assets,
    *fc_rankings_assets,
    *dp_rankings_assets,
    *dd_rankings_assets,
    *fn_rankings_history_assets,
    # projections assets
    *cbs_projections_assets,
    *espn_projections_assets,
    *nfl_projections_assets,
    ]

# jobs defintions
jobs = [
    ktc_rookies_job,
    fantasy_navigator_job,
    fantasy_navigator_hist_job,
    #projections jobs
    cbs_projections_job,
    espn_projections_job,
    nfl_projections_job,
]
# schedules definitions
schedules = [
    ktc_rookies_schedule,
    fantasy_navigator_schedule,
    cbs_projections_schedule,
    espn_projections_schedule,
    nfl_projections_schedule,
    fantasy_navigator_hist_schedule,
]

# Resource definitions
resource_defs = {
    "postgres": PostgreSQLResource(
        connection_string=postgres_connection_string
    )
}


defs = Definitions(
    assets=asset_defs,
    resources=resource_defs,
    jobs=jobs,
    schedules=schedules,
)
