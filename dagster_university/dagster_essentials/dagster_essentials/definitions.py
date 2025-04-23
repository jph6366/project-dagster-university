import dagster as dg

from dagster_essentials.assets import metrics, trips, zones
from dagster_essentials.resources import database_resource
from dagster_essentials.jobs import trip_update_job, weekly_update_job
from dagster_essentials.schedules import trip_update_schedule, weekly_update_schedule

trip_assets = dg.load_assets_from_modules([trips])
zones_assets = dg.load_assets_from_modules([zones])
metric_assets = dg.load_assets_from_modules([metrics])
all_jobs = [trip_update_job, weekly_update_job]
all_schedules = [trip_update_schedule, weekly_update_schedule]

defs = dg.Definitions(
    assets=[*zones_assets, *trip_assets, *metric_assets],
    resources={
        "database": database_resource,
    },
    jobs=all_jobs,
    schedules=all_schedules,
)
