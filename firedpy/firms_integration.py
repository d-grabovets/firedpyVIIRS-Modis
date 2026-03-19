"""
firedpy.firms_integration
=========================
Download NASA FIRMS active fire data and cross-validate with VNP64A1 events.

FIRMS VNP14IMG provides near-real-time hotspot detections (3-hour latency).
Cross-validating with VNP64A1 burned area significantly boosts confidence:
  - Events confirmed by FIRMS = "high" confidence
  - Events only in VNP64A1 = "medium" confidence (burned area only)
  - Hotspots only in FIRMS = "low" confidence (may be industrial/gas flare)

Requires NASA FIRMS API key: https://firms.modaps.eosdis.nasa.gov/api/area/
Free key gives 10 requests/minute, unlimited archive access.
"""

from __future__ import annotations

import io
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
from shapely.geometry import Point

logger = logging.getLogger(__name__)

# Default bbox: world. Override via --bbox or config.
# Default bbox: world. Override via --bbox or config.
DEFAULT_WEST, DEFAULT_SOUTH, DEFAULT_EAST, DEFAULT_NORTH = -180.0, -90.0, 180.0, 90.0

# FIRMS CSV columns (VIIRS VNP14IMG v2)
FIRMS_COLUMNS = [
    "latitude", "longitude", "bright_ti4", "scan", "track",
    "acq_date", "acq_time", "satellite", "instrument",
    "confidence", "version", "bright_ti5", "frp", "daynight",
]

# Some FIRMS CSVs use "acquire_time" instead of "acq_date"/"acq_time"
_COLUMN_ALIASES = {
    "acquire_time": "acq_date",  # will be split into date part
}


def download_firms_csv(
    api_key: str,
    start_date: str,
    end_date: str,
    bbox: tuple[float, float, float, float] = (DEFAULT_WEST, DEFAULT_SOUTH, DEFAULT_EAST, DEFAULT_NORTH),
    source: str = "VIIRS_SNPP_NRT",
    cache_dir: Optional[Path] = None,
) -> gpd.GeoDataFrame:
    """Download FIRMS hotspot CSV for a date range and return as GeoDataFrame.

    Parameters
    ----------
    api_key : str
        NASA FIRMS API key (free: https://firms.modaps.eosdis.nasa.gov/api/area/).
    start_date, end_date : str
        ISO format dates, e.g. "2024-01-01", "2024-12-31".
        Maximum range per request: 10 days for NRT, unlimited for archive.
    bbox : tuple
        (west, south, east, north) bounding box in WGS84.
    source : str
        FIRMS source. Options: "VIIRS_SNPP_NRT", "VIIRS_SNPP_SP",
        "VIIRS_NOAA20_NRT", "MODIS_NRT".
    cache_dir : Path, optional
        If provided, cache CSV to this directory for offline reuse.

    Returns
    -------
    GeoDataFrame with columns: latitude, longitude, acq_date, confidence, frp, geometry.
    """
    import requests

    west, south, east, north = bbox
    url = (
        f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
        f"{api_key}/{source}/{west},{south},{east},{north}/1/"
        f"{start_date}/{end_date}"
    )

    # Check cache first
    if cache_dir:
        cache_dir = Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_dir / f"firms_{source}_{start_date}_{end_date}.csv"
        if cache_file.exists():
            logger.info("Loading cached FIRMS: %s", cache_file)
            df = pd.read_csv(cache_file)
            return _firms_df_to_gdf(df)

    logger.info("Downloading FIRMS %s: %s to %s ...", source, start_date, end_date)
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    df = pd.read_csv(io.StringIO(resp.text))
    if df.empty:
        logger.warning("FIRMS returned 0 hotspots for %s - %s", start_date, end_date)
        return gpd.GeoDataFrame(
            columns=["latitude", "longitude", "acq_date", "confidence", "frp", "geometry"],
            geometry="geometry",
            crs="EPSG:4326",
        )

    # Cache
    if cache_dir:
        df.to_csv(cache_file, index=False)
        logger.info("Cached FIRMS CSV: %s (%d rows)", cache_file.name, len(df))

    return _firms_df_to_gdf(df)


def _firms_df_to_gdf(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert FIRMS DataFrame to GeoDataFrame.

    Handles both standard FIRMS format (acq_date + acq_time columns)
    and alternative format (acquire_time as datetime column).
    """
    if df.empty:
        return gpd.GeoDataFrame(
            columns=["latitude", "longitude", "acq_date", "confidence", "frp", "geometry"],
            geometry="geometry",
            crs="EPSG:4326",
        )

    df = df.copy()

    # Handle "acquire_time" column (datetime) → split into "acq_date"
    if "acquire_time" in df.columns and "acq_date" not in df.columns:
        df["acq_date"] = pd.to_datetime(df["acquire_time"]).dt.date
    if "acq_date" in df.columns:
        df["acq_date"] = pd.to_datetime(df["acq_date"])

    geometry = [Point(lon, lat) for lon, lat in zip(df["longitude"], df["latitude"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    return gdf


def download_firms_archive_chunked(
    api_key: str,
    year: int,
    bbox: tuple = (DEFAULT_WEST, DEFAULT_SOUTH, DEFAULT_EAST, DEFAULT_NORTH),
    source: str = "VIIRS_SNPP_SP",
    cache_dir: Optional[Path] = None,
    chunk_days: int = 10,
) -> gpd.GeoDataFrame:
    """Download full year of FIRMS data in chunks (API limit = 10 days/request).

    Parameters
    ----------
    api_key : str
        NASA FIRMS API key.
    year : int
        Year to download.
    chunk_days : int
        Days per API request (max 10 for NRT).

    Returns
    -------
    GeoDataFrame with all hotspots for the year.
    """
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    all_gdfs = []

    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        gdf = download_firms_csv(
            api_key=api_key,
            start_date=current.isoformat(),
            end_date=chunk_end.isoformat(),
            bbox=bbox,
            source=source,
            cache_dir=cache_dir,
        )
        if not gdf.empty:
            all_gdfs.append(gdf)
        current = chunk_end + timedelta(days=1)

    if not all_gdfs:
        return gpd.GeoDataFrame(
            columns=["latitude", "longitude", "acq_date", "confidence", "frp", "geometry"],
            geometry="geometry",
            crs="EPSG:4326",
        )

    combined = gpd.GeoDataFrame(
        pd.concat(all_gdfs, ignore_index=True),
        geometry="geometry",
        crs="EPSG:4326",
    )
    logger.info("Downloaded %d FIRMS hotspots for %d", len(combined), year)
    return combined


# ── Cross-validation ──────────────────────────────────────────────────────────

def cross_validate_with_firms(
    events_gdf: gpd.GeoDataFrame,
    firms_gdf: gpd.GeoDataFrame,
    radius_m: float = 1500.0,
    date_tolerance_days: int = 8,
) -> gpd.GeoDataFrame:
    """Cross-validate VNP64A1 burned area events with FIRMS hotspot detections.

    For each burned area event:
      - If ≥1 FIRMS hotspot falls within *radius_m* and within *date_tolerance_days*
        of the event's burn date → mark as FIRMS-confirmed
      - Count matching FIRMS detections → firms_count
      - Record earliest FIRMS detection → firms_first_date

    Parameters
    ----------
    events_gdf : GeoDataFrame
        VNP64A1 burned area events (EPSG:4326).
    firms_gdf : GeoDataFrame
        FIRMS hotspot detections (EPSG:4326).
    radius_m : float
        Spatial matching radius in metres.
    date_tolerance_days : int
        Temporal window for matching (±days from burn date).

    Returns
    -------
    events_gdf with added columns:
      firms_confirmed (bool), firms_count (int), firms_first_date (str),
      firms_max_frp (float), confidence_firms (str)
    """
    if events_gdf.empty:
        events_gdf["firms_confirmed"] = False
        events_gdf["firms_count"] = 0
        events_gdf["firms_first_date"] = None
        events_gdf["firms_max_frp"] = 0.0
        events_gdf["confidence_firms"] = "unverified"
        return events_gdf

    out = events_gdf.copy()

    if firms_gdf.empty:
        out["firms_confirmed"] = False
        out["firms_count"] = 0
        out["firms_first_date"] = None
        out["firms_max_frp"] = 0.0
        out["confidence_firms"] = "unverified"
        return out

    # Project to equal-area for distance calculation
    events_ea = out.to_crs("EPSG:6933")
    firms_ea = firms_gdf.to_crs("EPSG:6933")

    # Build KD-tree from FIRMS hotspots
    firms_coords = np.array(list(zip(firms_ea.geometry.x, firms_ea.geometry.y)))
    tree = cKDTree(firms_coords)

    # Event centroids
    event_centroids = np.array(
        list(zip(events_ea.geometry.centroid.x, events_ea.geometry.centroid.y))
    )

    # Parse dates
    event_dates = pd.to_datetime(out.get("ig_date", pd.Series(dtype=str))).to_numpy()
    firms_dates = pd.to_datetime(firms_gdf.get("acq_date", pd.Series(dtype=str))).to_numpy()
    firms_frp = firms_gdf.get("frp", pd.Series(0.0, index=firms_gdf.index)).to_numpy()

    confirmed = []
    counts = []
    first_dates = []
    max_frps = []

    for i in range(len(event_centroids)):
        nearby_idx = tree.query_ball_point(event_centroids[i], r=radius_m)
        if not nearby_idx:
            confirmed.append(False)
            counts.append(0)
            first_dates.append(None)
            max_frps.append(0.0)
            continue

        # Temporal filter
        ev_date = event_dates[i]
        if pd.isna(ev_date):
            confirmed.append(False)
            counts.append(0)
            first_dates.append(None)
            max_frps.append(0.0)
            continue

        time_diffs = np.abs(
            (firms_dates[nearby_idx] - ev_date).astype("timedelta64[D]").astype(float)
        )
        temporal_match = time_diffs <= date_tolerance_days
        matching_idx = [nearby_idx[j] for j in range(len(nearby_idx)) if temporal_match[j]]

        if matching_idx:
            confirmed.append(True)
            counts.append(len(matching_idx))
            first_dates.append(
                str(np.min(firms_dates[matching_idx]).astype("datetime64[D]"))
            )
            max_frps.append(float(np.max(firms_frp[matching_idx])))
        else:
            confirmed.append(False)
            counts.append(0)
            first_dates.append(None)
            max_frps.append(0.0)

    out["firms_confirmed"] = confirmed
    out["firms_count"] = counts
    out["firms_first_date"] = first_dates
    out["firms_max_frp"] = max_frps

    # Upgraded confidence label
    out["confidence_firms"] = "unverified"
    out.loc[out["firms_confirmed"], "confidence_firms"] = "high"
    out.loc[~out["firms_confirmed"] & (out.get("area_ha", 0) >= 100), "confidence_firms"] = "medium"
    out.loc[~out["firms_confirmed"] & (out.get("area_ha", 0) < 100), "confidence_firms"] = "low"

    n_conf = sum(confirmed)
    logger.info(
        "FIRMS cross-validation: %d/%d events confirmed (%.1f%%)",
        n_conf, len(out), n_conf / max(len(out), 1) * 100,
    )
    return out


def load_firms_local_csv(csv_path: Path) -> gpd.GeoDataFrame:
    """Load a locally saved FIRMS CSV file (from FIRMS web download)."""
    df = pd.read_csv(csv_path)
    return _firms_df_to_gdf(df)
