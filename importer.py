from sqlalchemy import create_engine, text
from config import CONFIG
import geopandas as gpd
import logging
import pandas as pd

def get_engine():
    pg = CONFIG["postgis"]
    return create_engine(
        f"postgresql://{pg['user']}:{pg['password']}"
        f"@{pg['host']}:{pg['port']}/{pg['db']}",
        future=True
    )

def import_to_staging(gdf, table_name):

    if len(gdf) == 0:
        raise ValueError("GeoDataFrame is empty, nothing to import")

    if not isinstance(gdf, gpd.GeoDataFrame):
        raise TypeError("Expected GeoDataFrame, got DataFrame")

    if "geometry" not in gdf.columns:
        raise ValueError("GeoDataFrame has no geometry column")

    gdf = gdf.set_geometry("geometry")

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE qgis."{table_name}"'))

    gdf.to_postgis(
        table_name,
        con=engine,
        schema="qgis",
        if_exists="append",
        index=False,
    )

    with engine.connect() as conn:
        count = conn.execute(
            text(f'SELECT COUNT(*) FROM qgis."{table_name}"')
        ).scalar()
        logging.info("Rows in %s after import: %s", table_name, count)

from sqlalchemy import text
import logging

def merge_to_production(staging_table, target_table):
    logging.info("Start merge from %s to %s", staging_table, target_table)

    engine = get_engine()

    with engine.begin() as conn:

        staging_count = conn.execute(
            text(f'SELECT COUNT(*) FROM qgis."{staging_table}"')
        ).scalar()

        if staging_count == 0:
            logging.warning("Staging table empty, merge skipped")
            return

        logging.info("Rows in staging: %s", staging_count)

        target_srid = conn.execute(
            text("""
            SELECT Find_SRID('qgis', :table, 'geom')
            """),
            {"table": target_table}
        ).scalar()

        logging.info("Target SRID: %s", target_srid)


        update_sql = text(f"""
        UPDATE qgis."{target_table}" tgt
        SET
          "Condition" = src."Condition",
          damage_level = src.damage_level,
          visual_symptoms = src.visual_symptoms,
          priority_level = src.priority_level,
          remarks = src.remarks,
          survey_date = src.survey_date,
          geom = ST_Transform(
                   ST_MakeValid(src.geometry),
                   :srid
                 )
        FROM qgis."{staging_table}" src
        WHERE tgt.id = src.id;
        """)

        result_update = conn.execute(update_sql, {"srid": target_srid})
        logging.info("Updated rows: %s", result_update.rowcount)

        insert_sql = text(f"""
        INSERT INTO qgis."{target_table}" (id, "Condition", damage_level, visual_symptoms, priority_level, remarks, survey_date, geom)
        SELECT
          s.id,
          s."Condition",
          s.damage_level,
          s.visual_symptoms,
          s.priority_level,
          s.remarks,
          CAST(s.survey_date as date),
          ST_Transform(
            ST_MakeValid(s.geometry),
            :srid
          )
        FROM qgis."{staging_table}" s
        WHERE NOT EXISTS (
          SELECT 1 FROM qgis."{target_table}" t WHERE t.id = s.id
        );
        """)

        result_insert = conn.execute(insert_sql, {"srid": target_srid})
        logging.info("Inserted rows: %s", result_insert.rowcount)

    logging.info("Merge completed successfully")