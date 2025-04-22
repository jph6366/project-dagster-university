import dagster as dg

from dagster_essentials.assets import metrics, trips, zones

trip_assets = dg.load_assets_from_modules([zones])
zones_assets = dg.load_assets_from_modules([trips])
metric_assets = dg.load_assets_from_modules([metrics])

defs = dg.Definitions(
    assets=[*zones_assets, *trip_assets, *metric_assets],
)
