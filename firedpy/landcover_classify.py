"""
firedpy.landcover_classify
==========================
Classify fire events by dominant land cover type using ESA WorldCover 2021.

Adds land_cover_type and fire_type fields to event GeoDataFrames.
Used for seasonal baseline filtering and operational reporting.

ESA WorldCover 2021 classes:
  10 = Tree cover           → "forest"
  20 = Shrubland            → "shrubland"
  30 = Grassland            → "grassland"
  40 = Cropland             → "cropland"
  50 = Built-up             → "urban"
  60 = Bare / sparse veg    → "bare"
  70 = Snow and ice         → "snow_ice"
  80 = Permanent water      → "water"
  90 = Herbaceous wetland   → "wetland"
  95 = Mangroves            → "mangroves"
 100 = Moss and lichen      → "moss_lichen"

Requires pre-processed WorldCover raster aligned to VIIRS grid (EPSG:6933, 500m).
Generate with: scripts/build_viirs_qa_worldcover_vs_fired_qgis.py
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import rowcol

logger = logging.getLogger(__name__)

# ESA WorldCover 2021 class labels
WC_LABELS = {
    10: "forest",
    20: "shrubland",
    30: "grassland",
    40: "cropland",
    50: "urban",
    60: "bare",
    70: "snow_ice",
    80: "water",
    90: "wetland",
    95: "mangroves",
    100: "moss_lichen",
}

# Fire type mapping from dominant land cover
_FIRE_TYPE = {
    "forest": "wildfire",
    "shrubland": "wildfire",
    "grassland": "grassfire",
    "cropland": "agricultural",
    "urban": "urban_industrial",
    "wetland": "peatfire",
    "bare": "unknown",
    "water": "false_positive",
    "snow_ice": "false_positive",
    "moss_lichen": "grassfire",
    "mangroves": "wildfire",
}

# Seasonal pattern: month ranges for typical burning by fire type
_SEASONAL_WINDOWS = {
    "agricultural": {
        "spring": (3, 5),   # March-May: stubble burning
        "autumn": (8, 10),  # Aug-Oct: post-harvest
    },
    "wildfire": {
        "fire_season": (6, 10),  # June-October
    },
    "grassfire": {
        "spring": (3, 5),
        "summer": (6, 9),
    },
    "peatfire": {
        "fire_season": (5, 10),
    },
}


def classify_events_by_landcover(
    events_gdf: gpd.GeoDataFrame,
    worldcover_path: Path,
    sample_points: int = 50,
) -> gpd.GeoDataFrame:
    """Add land cover classification to fire events.

    For each event polygon, samples up to *sample_points* locations within
    the polygon and reads the WorldCover class at each point. The dominant
    class becomes the event's land_cover_type.

    Parameters
    ----------
    events_gdf : GeoDataFrame
        Fire events in any CRS (will be reprojected to match WorldCover).
    worldcover_path : Path
        Pre-processed WorldCover raster (EPSG:6933 or EPSG:4326).
    sample_points : int
        Max points to sample per polygon for classification.

    Returns
    -------
    GeoDataFrame with added columns:
      land_cover_class (int), land_cover_type (str), fire_type (str),
      lc_forest_pct (float), lc_cropland_pct (float), seasonal_match (bool)
    """
    if events_gdf.empty:
        for col in ("land_cover_class", "land_cover_type", "fire_type",
                     "lc_forest_pct", "lc_cropland_pct", "seasonal_match"):
            events_gdf[col] = None
        return events_gdf

    out = events_gdf.copy()

    with rasterio.open(worldcover_path) as ds:
        wc_crs = ds.crs
        wc_data = ds.read(1)
        wc_transform = ds.transform

    # Reproject events to WorldCover CRS
    if out.crs and str(out.crs) != str(wc_crs):
        events_proj = out.to_crs(wc_crs)
    else:
        events_proj = out

    lc_classes = []
    lc_types = []
    fire_types = []
    forest_pcts = []
    cropland_pcts = []
    seasonal_matches = []

    for idx in events_proj.index:
        geom = events_proj.loc[idx, "geometry"]
        if geom is None or geom.is_empty:
            lc_classes.append(0)
            lc_types.append("unknown")
            fire_types.append("unknown")
            forest_pcts.append(0.0)
            cropland_pcts.append(0.0)
            seasonal_matches.append(False)
            continue

        # Sample points within the polygon
        classes = _sample_worldcover(geom, wc_data, wc_transform, sample_points)

        if len(classes) == 0:
            lc_classes.append(0)
            lc_types.append("unknown")
            fire_types.append("unknown")
            forest_pcts.append(0.0)
            cropland_pcts.append(0.0)
            seasonal_matches.append(False)
            continue

        # Dominant class
        unique, counts = np.unique(classes, return_counts=True)
        dominant = unique[np.argmax(counts)]
        total = len(classes)

        lc_label = WC_LABELS.get(dominant, "unknown")
        ft = _FIRE_TYPE.get(lc_label, "unknown")

        lc_classes.append(int(dominant))
        lc_types.append(lc_label)
        fire_types.append(ft)

        # Percentages
        forest_pcts.append(
            round(np.sum(classes == 10) / total * 100, 1) if total > 0 else 0.0
        )
        cropland_pcts.append(
            round(np.sum(classes == 40) / total * 100, 1) if total > 0 else 0.0
        )

        # Seasonal match
        month = out.loc[idx].get("month", 0)
        seasonal_matches.append(_check_seasonal(ft, int(month) if pd.notna(month) else 0))

    out["land_cover_class"] = lc_classes
    out["land_cover_type"] = lc_types
    out["fire_type"] = fire_types
    out["lc_forest_pct"] = forest_pcts
    out["lc_cropland_pct"] = cropland_pcts
    out["seasonal_match"] = seasonal_matches

    # Log summary
    type_counts = pd.Series(fire_types).value_counts()
    logger.info("Land cover classification: %s", dict(type_counts))

    n_seasonal = sum(seasonal_matches)
    logger.info("Seasonal match: %d/%d events (%.1f%%)",
                n_seasonal, len(out), n_seasonal / max(len(out), 1) * 100)

    return out


def _sample_worldcover(
    geom, wc_data: np.ndarray, wc_transform, max_points: int
) -> np.ndarray:
    """Sample WorldCover values at points within a polygon."""
    from shapely.geometry import MultiPolygon, Polygon

    # Use centroid + bounds-based grid sampling
    minx, miny, maxx, maxy = geom.bounds

    # Grid spacing based on polygon size
    dx = max((maxx - minx) / 8, abs(wc_transform.a))
    dy = max((maxy - miny) / 8, abs(wc_transform.e))

    xs = np.arange(minx + dx / 2, maxx, dx)
    ys = np.arange(miny + dy / 2, maxy, dy)

    if len(xs) == 0 or len(ys) == 0:
        # Fallback to centroid
        cx, cy = geom.centroid.x, geom.centroid.y
        xs, ys = [cx], [cy]

    classes = []
    from shapely.geometry import Point

    for x in xs:
        for y in ys:
            if len(classes) >= max_points:
                break
            pt = Point(x, y)
            if geom.contains(pt):
                try:
                    row, col = rowcol(wc_transform, x, y)
                    if 0 <= row < wc_data.shape[0] and 0 <= col < wc_data.shape[1]:
                        val = wc_data[row, col]
                        if val > 0:
                            classes.append(val)
                except (IndexError, ValueError):
                    pass

    # Always include centroid
    if not classes:
        cx, cy = geom.centroid.x, geom.centroid.y
        try:
            row, col = rowcol(wc_transform, cx, cy)
            if 0 <= row < wc_data.shape[0] and 0 <= col < wc_data.shape[1]:
                val = wc_data[row, col]
                if val > 0:
                    classes.append(val)
        except (IndexError, ValueError):
            pass

    return np.array(classes, dtype=np.uint8)


def _check_seasonal(fire_type: str, month: int) -> bool:
    """Check if the fire occurred in a typical seasonal window for its type."""
    if month == 0 or fire_type not in _SEASONAL_WINDOWS:
        return True  # No seasonal data or unknown type → no penalty

    windows = _SEASONAL_WINDOWS[fire_type]
    for _, (start_month, end_month) in windows.items():
        if start_month <= month <= end_month:
            return True
    return False
