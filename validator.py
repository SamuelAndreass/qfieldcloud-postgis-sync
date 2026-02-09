import geopandas as gpd
from config import CONFIG

ALLOWED_COLUMNS = {
    "id",
    "Condition",
    "geometry",
    "damage_level",
    "visual_symptoms",
    "priority_level",
    "remarks",
    "survey_date",
}

REQUIRED_CRS = "EPSG:32647"

def validate_layer(gpkg_path: str, layer_name: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(gpkg_path, layer=layer_name)

    if gdf.empty:
        raise RuntimeError("Layer is empty")

    if "geometry" not in gdf.columns:
        raise ValueError("Layer has no geometry")

    if len(gdf) > CONFIG["pipeline"]["max_features"]:
        raise RuntimeError("Feature count exceeds limit")

    if gdf.crs is None:
        raise RuntimeError("Layer has no CRS")

    if gdf.crs.to_string() != REQUIRED_CRS:
        gdf = gdf.to_crs(REQUIRED_CRS)

    keep_cols = [c for c in gdf.columns if c in ALLOWED_COLUMNS]
    gdf = gdf[keep_cols]

    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=REQUIRED_CRS)

    if gdf.empty:
        raise RuntimeError("No valid features after validation")

    return gdf