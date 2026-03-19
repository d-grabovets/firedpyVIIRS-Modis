"""
GeoJSON export utilities for firedpy burned-area event GeoDataFrames.

Outputs web-ready GeoJSON (EPSG:4326) with optional topology-preserving
simplification. Designed for Leaflet / MapLibre GL JS display.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)

# Columns to keep in web output (drop heavy intermediate columns)
_WEB_COLUMNS = {
    "id", "ig_date", "last_date", "area_ha", "tot_ar_km2",
    "month", "year", "doy_start", "doy_end", "duration_days",
    "source", "source_product", "qa_mode", "resolution_m",
    "confidence", "confidence_score", "confidence_label", "qa_pixel_ratio",
    "confidence_firms",
    "conflict_zone_consensus", "front_distance_km",
    "firms_confirmed", "firms_count", "firms_first_date",
    "firms_max_frp", "firms_first_detection",
    "patch_count",
    "land_cover_class", "land_cover_type", "fire_type",
    "lc_forest_pct", "lc_cropland_pct", "seasonal_match",
    "geometry",
}


def to_geojson(
    gdf: gpd.GeoDataFrame,
    output_path: Path | str,
    simplify_tolerance: float | None = 0.001,
    keep_columns: set[str] | None = None,
    crs: str = "EPSG:4326",
) -> Path:
    """
    Export a firedpy event GeoDataFrame to a web-ready GeoJSON file.

    Parameters
    ----------
    gdf : GeoDataFrame
        Fire event polygons from fired() or fired_stitched().
    output_path : Path | str
        Destination .geojson file path.
    simplify_tolerance : float | None
        Simplification tolerance in CRS units (degrees for EPSG:4326).
        0.001° ≈ 100m. Set None to disable simplification.
        Uses topology-preserving simplification (preserve_topology=True).
    keep_columns : set[str] | None
        Column names to include. Defaults to _WEB_COLUMNS.
        'geometry' is always included.
    crs : str
        Output CRS. Defaults to EPSG:4326 (required by GeoJSON spec).

    Returns
    -------
    Path
        Resolved path to the written GeoJSON file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if gdf is None or gdf.empty:
        logger.warning("to_geojson: empty GeoDataFrame, writing empty FeatureCollection.")
        empty_fc = {"type": "FeatureCollection", "features": []}
        output_path.write_text(json.dumps(empty_fc), encoding="utf-8")
        return output_path

    out = gdf.copy()

    # Reproject to output CRS
    if out.crs is None or str(out.crs) != crs:
        out = out.to_crs(crs)

    # Select columns
    cols = (keep_columns or _WEB_COLUMNS) | {"geometry"}
    present = [c for c in cols if c in out.columns]
    out = out[present].copy()

    # Simplify geometries
    if simplify_tolerance is not None and simplify_tolerance > 0:
        out["geometry"] = out["geometry"].simplify(
            simplify_tolerance, preserve_topology=True
        )

    # Serialize date columns to ISO strings for JSON compatibility
    for col in ("ig_date", "last_date", "firms_first_detection"):
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce").dt.strftime("%Y-%m-%d")

    out.to_file(output_path, driver="GeoJSON")
    logger.info("GeoJSON written: %s (%d features)", output_path, len(out))
    return output_path.resolve()


def to_geojson_daily_sequence(
    daily_gdf: gpd.GeoDataFrame,
    output_dir: Path | str,
    simplify_tolerance: float | None = 0.001,
    crs: str = "EPSG:4326",
) -> list[Path]:
    """
    Export daily fire progression polygons as one GeoJSON file per date.

    Creates files like ``output_dir/2024-05-15.geojson``.
    Suitable for Leaflet.TimeDimension or custom timeline sliders.

    Parameters
    ----------
    daily_gdf : GeoDataFrame
        Daily progression polygons (requires 'date' or 'ig_date' column).
    output_dir : Path | str
        Directory to write per-day GeoJSON files into.
    simplify_tolerance : float | None
        Simplification tolerance (degrees). Set None to disable.
    crs : str
        Output CRS.

    Returns
    -------
    list[Path]
        Sorted list of written GeoJSON file paths.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if daily_gdf is None or daily_gdf.empty:
        logger.warning("to_geojson_daily_sequence: empty GeoDataFrame, nothing written.")
        return []

    date_col = next(
        (c for c in ("date", "ig_date", "ignition_date") if c in daily_gdf.columns),
        None,
    )
    if date_col is None:
        raise KeyError(
            "daily_gdf requires a 'date', 'ig_date', or 'ignition_date' column."
        )

    out = daily_gdf.copy()
    if out.crs is None or str(out.crs) != crs:
        out = out.to_crs(crs)

    if simplify_tolerance is not None and simplify_tolerance > 0:
        out["geometry"] = out["geometry"].simplify(
            simplify_tolerance, preserve_topology=True
        )

    dates = pd.to_datetime(out[date_col], errors="coerce").dt.date.unique()
    written = []
    for d in sorted(dates):
        day_gdf = out[pd.to_datetime(out[date_col], errors="coerce").dt.date == d]
        if day_gdf.empty:
            continue
        fpath = output_dir / f"{d}.geojson"
        day_gdf.to_file(fpath, driver="GeoJSON")
        written.append(fpath.resolve())

    logger.info(
        "Daily GeoJSON sequence: %d files written to %s", len(written), output_dir
    )
    return written
