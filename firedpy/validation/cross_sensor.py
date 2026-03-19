"""
Cross-sensor validation: compare VIIRS and MODIS burned area events.

Matches events by spatial overlap (IoU) and temporal proximity (Δdate),
then computes validation metrics.
"""

import logging
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.ops import unary_union

logger = logging.getLogger(__name__)


def match_events(
    viirs_gdf: gpd.GeoDataFrame,
    modis_gdf: gpd.GeoDataFrame,
    iou_threshold: float = 0.1,
    date_tolerance_days: int = 10,
    date_col: str = "ig_date",
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Match VIIRS events to MODIS events by spatial overlap + temporal proximity.

    Parameters
    ----------
    viirs_gdf : GeoDataFrame of VIIRS fire events
    modis_gdf : GeoDataFrame of MODIS fire events
    iou_threshold : minimum Intersection-over-Union to consider a match
    date_tolerance_days : max days between ignition dates
    date_col : column name for ignition date

    Returns
    -------
    matched : GeoDataFrame — pairs with columns from both + metrics
    viirs_only : GeoDataFrame — VIIRS events with no MODIS match
    modis_only : GeoDataFrame — MODIS events with no VIIRS match
    """
    # Ensure same CRS (project to equal-area for IoU)
    target_crs = "EPSG:6933"
    v = viirs_gdf.to_crs(target_crs).copy()
    m = modis_gdf.to_crs(target_crs).copy()

    v["_v_idx"] = range(len(v))
    m["_m_idx"] = range(len(m))

    # Spatial join (intersects)
    joined = gpd.sjoin(v, m, how="inner", predicate="intersects")

    matches = []
    matched_v = set()
    matched_m = set()

    for _, row in joined.iterrows():
        v_idx = row["_v_idx"]
        m_idx = row["_m_idx"]  # from index_right

        if v_idx in matched_v or m_idx in matched_m:
            continue

        v_geom = v.loc[v["_v_idx"] == v_idx].geometry.iloc[0]
        m_geom = m.loc[m["_m_idx"] == m_idx].geometry.iloc[0]

        # Compute IoU
        intersection = v_geom.intersection(m_geom).area
        union = v_geom.area + m_geom.area - intersection
        iou = intersection / union if union > 0 else 0

        if iou < iou_threshold:
            continue

        # Check temporal proximity
        v_date = pd.to_datetime(v.loc[v["_v_idx"] == v_idx, date_col].iloc[0])
        m_date = pd.to_datetime(m.loc[m["_m_idx"] == m_idx, date_col].iloc[0])
        delta_days = abs((v_date - m_date).days)

        if delta_days > date_tolerance_days:
            continue

        matches.append({
            "viirs_idx": v_idx,
            "modis_idx": m_idx,
            "iou": round(iou, 4),
            "delta_days": delta_days,
            "area_viirs_ha": v_geom.area / 10000,
            "area_modis_ha": m_geom.area / 10000,
            "area_bias": round((v_geom.area - m_geom.area) / m_geom.area, 4) if m_geom.area > 0 else None,
            "viirs_date": v_date,
            "modis_date": m_date,
            "geometry": v_geom,  # Use VIIRS geometry for the match
        })
        matched_v.add(v_idx)
        matched_m.add(m_idx)

    if matches:
        matched_gdf = gpd.GeoDataFrame(matches, crs=target_crs)
    else:
        matched_gdf = gpd.GeoDataFrame(columns=[
            "viirs_idx", "modis_idx", "iou", "delta_days",
            "area_viirs_ha", "area_modis_ha", "area_bias",
            "viirs_date", "modis_date", "geometry"
        ])

    viirs_only = v[~v["_v_idx"].isin(matched_v)].drop(columns=["_v_idx"])
    modis_only = m[~m["_m_idx"].isin(matched_m)].drop(columns=["_m_idx"])

    logger.info(
        "Cross-sensor matching: %d matched, %d VIIRS-only, %d MODIS-only",
        len(matched_gdf), len(viirs_only), len(modis_only),
    )

    return matched_gdf, viirs_only, modis_only


def validation_report(
    matched: gpd.GeoDataFrame,
    viirs_only: gpd.GeoDataFrame,
    modis_only: gpd.GeoDataFrame,
    viirs_total: int,
    modis_total: int,
) -> dict:
    """
    Generate summary statistics for cross-sensor comparison.

    Returns dict with:
      - total_viirs, total_modis
      - n_matched, n_viirs_only, n_modis_only
      - match_rate_viirs, match_rate_modis
      - median_iou, mean_iou
      - median_delta_days, mean_delta_days
      - median_area_bias, mean_area_bias
      - area_correlation (Pearson r between matched areas)
    """
    report = {
        "total_viirs": viirs_total,
        "total_modis": modis_total,
        "n_matched": len(matched),
        "n_viirs_only": len(viirs_only),
        "n_modis_only": len(modis_only),
        "match_rate_viirs": round(len(matched) / viirs_total, 4) if viirs_total > 0 else 0,
        "match_rate_modis": round(len(matched) / modis_total, 4) if modis_total > 0 else 0,
    }

    if len(matched) > 0:
        report["median_iou"] = round(matched["iou"].median(), 4)
        report["mean_iou"] = round(matched["iou"].mean(), 4)
        report["median_delta_days"] = matched["delta_days"].median()
        report["mean_delta_days"] = round(matched["delta_days"].mean(), 2)
        report["median_area_bias"] = round(matched["area_bias"].median(), 4)
        report["mean_area_bias"] = round(matched["area_bias"].mean(), 4)

        # Pearson correlation of matched areas
        if len(matched) > 2:
            from scipy.stats import pearsonr
            try:
                r, p = pearsonr(matched["area_viirs_ha"], matched["area_modis_ha"])
                report["area_correlation_r"] = round(r, 4)
                report["area_correlation_p"] = round(p, 6)
            except Exception:
                report["area_correlation_r"] = None
                report["area_correlation_p"] = None

    return report


def print_report(report: dict) -> str:
    """Format validation report as readable text."""
    lines = [
        "=" * 60,
        "CROSS-SENSOR VALIDATION REPORT (VIIRS vs MODIS)",
        "=" * 60,
        f"VIIRS events: {report['total_viirs']}",
        f"MODIS events: {report['total_modis']}",
        f"Matched pairs: {report['n_matched']}",
        f"VIIRS-only: {report['n_viirs_only']}",
        f"MODIS-only: {report['n_modis_only']}",
        "",
        f"Match rate (VIIRS): {report['match_rate_viirs']:.1%}",
        f"Match rate (MODIS): {report['match_rate_modis']:.1%}",
    ]

    if report["n_matched"] > 0:
        lines += [
            "",
            "--- Spatial Metrics ---",
            f"Median IoU: {report.get('median_iou', 'N/A')}",
            f"Mean IoU:   {report.get('mean_iou', 'N/A')}",
            "",
            "--- Temporal Metrics ---",
            f"Median Δdate: {report.get('median_delta_days', 'N/A')} days",
            f"Mean Δdate:   {report.get('mean_delta_days', 'N/A')} days",
            "",
            "--- Area Metrics ---",
            f"Median area bias: {report.get('median_area_bias', 'N/A')}",
            f"Mean area bias:   {report.get('mean_area_bias', 'N/A')}",
        ]
        if report.get("area_correlation_r") is not None:
            lines.append(f"Area correlation: r={report['area_correlation_r']}, p={report['area_correlation_p']}")

    lines.append("=" * 60)
    return "\n".join(lines)
