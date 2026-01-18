import logging
from downloader import download_latest_gpkg
from validator import validate_layer
from importer import import_to_staging, merge_to_production

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

LAYER_NAME = "palmTree"
STAGING_TABLE = "palmTree_staging"
TARGET_TABLE = "palmTree"


def run():
    logging.info("Starting QField pipeline")
    try:
        gpkg = download_latest_gpkg()
        gdf = validate_layer(gpkg, LAYER_NAME)
        logging.info("STEP X len(gdf): %s", len(gdf))
        import_to_staging(gdf, STAGING_TABLE)
        merge_to_production(STAGING_TABLE, TARGET_TABLE)
        logging.info("Pipeline finished successfully")
    except Exception:
        logging.exception("Pipeline failed")
        raise SystemExit(1)


if __name__ == "__main__":
    run()
