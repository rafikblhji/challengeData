from dagster import Definitions, load_assets_from_modules

from rafik_pipeline import assets  

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
)
