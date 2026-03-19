"""
firedpy.event_quality
=====================
Post-vectorization quality improvements for fire event GeoDataFrames.
All functions operate on EPSG:6933 (equal-area metres) GeoDataFrames.

No external reference data required — works without GABAM on the server.
"""

from __future__ import annotations

import logging
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import unary_union
from shapely.validation import make_valid

logger = logging.getLogger(__name__)


# ── 1. Geometry repair ──────────────────────────────────────────────────────

def repair_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Fix invalid geometries using shapely.make_valid.

    Returns a copy with all geometries valid. Rows whose geometry cannot be
    repaired (empty after make_valid) are dropped with a warning.
    """
    if gdf.empty:
        return gdf

    out = gdf.copy()
    invalid_mask = ~out.geometry.is_valid
    n_invalid = invalid_mask.sum()
    if n_invalid:
        logger.warning("Repairing %d invalid geometries.", n_invalid)
        out.loc[invalid_mask, "geometry"] = out.loc[invalid_mask, "geometry"].apply(make_valid)

    # Drop anything that became empty after repair
    empty_mask = out.geometry.is_empty | out.geometry.isna()
    n_empty = empty_mask.sum()
    if n_empty:
        logger.warning("Dropping %d empty/null geometries after repair.", n_empty)
        out = out[~empty_mask].copy()

    return out.reset_index(drop=True)


# ── 2. Minimum area filter ───────────────────────────────────────────────────

def filter_min_area(
    gdf: gpd.GeoDataFrame,
    min_area_ha: float = 25.0,
    area_col: str = "area_ha",
) -> gpd.GeoDataFrame:
    """Drop patches smaller than *min_area_ha* hectares.

    VIIRS 500m pixel = 25 ha. Keeping 1-pixel patches adds noise.
    Default threshold = 25 ha removes single-pixel artifacts.
    """
    if gdf.empty:
        return gdf

    if area_col in gdf.columns:
        mask = gdf[area_col] >= min_area_ha
    else:
        # Recompute from geometry — use equal-area CRS for correct hectares
        if gdf.crs and gdf.crs.is_geographic:
            area_m2 = gdf.to_crs("EPSG:6933").geometry.area
        else:
            area_m2 = gdf.geometry.area
        mask = (area_m2 / 10_000) >= min_area_ha

    dropped = (~mask).sum()
    if dropped:
        logger.info("Dropped %d patches below %.0f ha threshold.", dropped, min_area_ha)

    return gdf[mask].copy().reset_index(drop=True)


# ── 3. Near-neighbor event merge ─────────────────────────────────────────────

def merge_nearby_patches(
    gdf: gpd.GeoDataFrame,
    buffer_m: float = 1000.0,
    date_tolerance_days: int = 5,
    doy_start_col: str = "doy_start",
    doy_end_col: str = "doy_end",
) -> gpd.GeoDataFrame:
    """Merge spatially adjacent patches that burned within *date_tolerance_days*.

    Strategy:
      1. Buffer each polygon by *buffer_m*.
      2. Spatial join with itself to find candidate pairs.
      3. Accept pairs where |ig_date_A - ig_date_B| <= date_tolerance_days (using calendar dates).
      4. Union-find to collect connected components.
      5. Dissolve each component, recompute area and DOY stats.

    Uses calendar dates (ig_date) instead of raw DOY to correctly handle fires
    spanning year boundaries (e.g., DOY 355 vs DOY 5).

    This is the primary substitute for GABAM cross-referencing: it merges
    spatially fragmented patches from the same fire event.
    """
    if gdf.empty or len(gdf) < 2:
        return gdf

    gdf = gdf.copy().reset_index(drop=True)
    # Buffer must be in metres — project to equal-area CRS if geographic
    original_crs = gdf.crs
    if gdf.crs and gdf.crs.is_geographic:
        gdf = gdf.to_crs("EPSG:6933")
    buffered = gdf.copy()
    buffered["geometry"] = buffered.geometry.buffer(buffer_m)

    # Spatial self-join
    joined = gpd.sjoin(
        buffered[["geometry", doy_start_col]],
        buffered[["geometry", doy_start_col]].rename(columns={doy_start_col: f"{doy_start_col}_r"}),
        how="inner",
        predicate="intersects",
    )
    # Remove self-joins
    joined = joined[joined.index != joined["index_right"]].copy()

    # Date proximity filter: use calendar dates instead of raw DOY for temporal comparison
    # This correctly handles fires spanning year boundaries (e.g., DOY 355 vs DOY 5)
    if "ig_date" in gdf.columns and "ig_date" in buffered.columns:
        # Add ig_date columns to joined dataframe for date-based comparison
        joined = joined.join(buffered[["ig_date"]].rename(columns={"ig_date": "ig_date_left"}), how="left")
        joined = joined.join(
            buffered[["ig_date"]].rename(columns={"ig_date": "ig_date_right"}),
            on="index_right",
            how="left"
        )
        # Convert to datetime and compute calendar date difference
        if joined["ig_date_left"].notna().any() and joined["ig_date_right"].notna().any():
            date_left = pd.to_datetime(joined["ig_date_left"])
            date_right = pd.to_datetime(joined["ig_date_right"])
            joined["date_diff"] = (date_left - date_right).dt.days.abs()
            pairs = joined[joined["date_diff"] <= date_tolerance_days]
        else:
            # Fallback to DOY if dates missing
            joined["doy_diff"] = (
                joined[doy_start_col] - joined[f"{doy_start_col}_r"]
            ).abs()
            pairs = joined[joined["doy_diff"] <= date_tolerance_days]
    else:
        # Fallback to DOY if no calendar dates available
        joined["doy_diff"] = (
            joined[doy_start_col] - joined[f"{doy_start_col}_r"]
        ).abs()
        pairs = joined[joined["doy_diff"] <= date_tolerance_days]

    # Union-Find
    parent = list(range(len(gdf)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for idx, row in pairs.iterrows():
        union(int(idx), int(row["index_right"]))

    gdf["_group"] = [find(i) for i in range(len(gdf))]

    # Dissolve by group
    dissolved_rows = []
    for grp_id, grp in gdf.groupby("_group"):
        geom = make_valid(unary_union(grp.geometry))
        row = grp.iloc[0].to_dict()
        row["geometry"] = geom
        # Aggregate DOY / date stats
        if doy_start_col in grp.columns:
            row[doy_start_col] = int(grp[doy_start_col].min())
        if doy_end_col in grp.columns:
            row[doy_end_col] = int(grp[doy_end_col].max())
        if "ig_date" in grp.columns:
            row["ig_date"] = grp["ig_date"].min()
        if "last_date" in grp.columns:
            row["last_date"] = grp["last_date"].max()
        if "area_ha" in grp.columns:
            row["area_ha"] = round(grp["area_ha"].sum(), 2)
        row["patch_count"] = len(grp)
        dissolved_rows.append(row)

    result = gpd.GeoDataFrame(dissolved_rows, geometry="geometry", crs=gdf.crs)
    result = result.drop(columns=["_group"], errors="ignore")

    # Recompute area in equal-area CRS
    if result.crs and not result.crs.is_geographic:
        result["area_ha"] = (result.geometry.area / 10_000).round(2)

    # Convert back to original CRS if we reprojected
    if original_crs and original_crs.is_geographic and result.crs != original_crs:
        result = result.to_crs(original_crs)

    n_before = len(gdf)
    n_after = len(result)
    logger.info(
        "merge_nearby_patches: %d patches → %d events (merged %d).",
        n_before, n_after, n_before - n_after,
    )
    return result.reset_index(drop=True)


# ── 4. Duration and timing fields ────────────────────────────────────────────

def add_duration_fields(
    gdf: gpd.GeoDataFrame,
    doy_start_col: str = "doy_start",
    doy_end_col: str = "doy_end",
) -> gpd.GeoDataFrame:
    """Add duration_days, ig_date, last_date derived from DOY columns."""
    if gdf.empty:
        return gdf

    out = gdf.copy()

    if doy_start_col in out.columns and doy_end_col in out.columns:
        out["duration_days"] = (
            out[doy_end_col] - out[doy_start_col] + 8  # +8 for 8-day composite
        ).clip(lower=1)

    # Fill ig_date / last_date from DOY if missing
    if "year" in out.columns:
        if "ig_date" not in out.columns or out["ig_date"].isna().any():
            out["ig_date"] = out.apply(
                lambda r: _doy_to_date(int(r.get("year", 2024)), int(r[doy_start_col]))
                if pd.notna(r.get(doy_start_col)) else None,
                axis=1,
            )
        if "last_date" not in out.columns or out["last_date"].isna().any():
            out["last_date"] = out.apply(
                lambda r: _doy_to_date(int(r.get("year", 2024)), int(r[doy_end_col]))
                if pd.notna(r.get(doy_end_col)) else None,
                axis=1,
            )

    return out


def _doy_to_date(year: int, doy: int) -> str:
    import datetime
    return str(datetime.date(year, 1, 1) + datetime.timedelta(days=int(doy) - 1))


# ── 5. Geometry simplification for web ──────────────────────────────────────

def simplify_for_web(
    gdf: gpd.GeoDataFrame,
    tolerance_m: float = 250.0,
    preserve_topology: bool = True,
) -> gpd.GeoDataFrame:
    """Simplify geometries for web tile rendering.

    Reduces vertex count while preserving topology.
    *tolerance_m*: simplification tolerance in metres (CRS must be metric).
    Recommended: 250m for leaflet zoom 8+, 500m for zoom 6-7.
    """
    if gdf.empty:
        return gdf

    out = gdf.copy()
    out["geometry"] = out.geometry.simplify(
        tolerance_m, preserve_topology=preserve_topology
    )
    # Re-validate after simplification
    out = repair_geometries(out)
    return out


# ── 6. Perimeter refinement via convex-hull ratio ────────────────────────────

def refine_perimeters(
    gdf: gpd.GeoDataFrame,
    convexity_threshold: float = 0.3,
) -> gpd.GeoDataFrame:
    """Refine fire perimeters by replacing jagged raster artifacts.

    VIIRS 500m pixels create staircase polygons. For large events, the
    convex hull is often a better approximation of the actual fire shape.
    For small events with complex real shapes, keep the original.

    Strategy:
      - Compute convexity ratio = area(polygon) / area(convex_hull)
      - If ratio < threshold → polygon is very jagged (raster artifact)
        → replace with alpha-shape (concave hull) or buffered convex hull
      - If ratio >= threshold → shape is reasonable, keep original
    """
    if gdf.empty:
        return gdf

    # Work in projected CRS for buffer operations
    original_crs = gdf.crs
    if gdf.crs and gdf.crs.is_geographic:
        out = gdf.to_crs("EPSG:6933").copy()
    else:
        out = gdf.copy()

    for idx in out.index:
        geom = out.loc[idx, "geometry"]
        if geom is None or geom.is_empty:
            continue

        hull = geom.convex_hull
        if hull.area == 0:
            continue

        convexity = geom.area / hull.area

        if convexity < convexity_threshold:
            refined = hull.buffer(-50).buffer(50)  # morphological close
            if refined.is_valid and not refined.is_empty and refined.area > 0:
                out.loc[idx, "geometry"] = refined

    # Recompute area in projected CRS
    if "area_ha" in out.columns:
        out["area_ha"] = (out.geometry.area / 10_000).round(2)

    # Convert back to original CRS
    if original_crs and original_crs.is_geographic:
        out = out.to_crs(original_crs)

    return out


# ── 7. Confidence scoring (no GABAM required) ────────────────────────────────

def add_confidence_scores_intrinsic(
    gdf: gpd.GeoDataFrame,
    doy_start_col: str = "doy_start",
    doy_end_col: str = "doy_end",
    area_col: str = "area_ha",
    qa_mode_col: str = "qa_mode",
) -> gpd.GeoDataFrame:
    """Assign intrinsic confidence label without GABAM.

    Calibrated for operational use — aims for ~30/50/20 high/medium/low
    distribution on typical VIIRS burned area data.

    Score components:
      - Area: 50ha→0.3, 200ha→0.5, 1000ha→0.7, 5000ha→0.9
      - Duration: multi-period detection boosts score
      - Patch count: merged events (patch_count>1) get bonus
      - QA mode: strict=+0.1, standard=0, permissive=-0.05
      - Uncertainty: low uncertainty (≤2 days) +0.1, normal (3-5) no change, high (>5) -0.1

    Labels: "high" (≥0.55), "medium" (0.3-0.55), "low" (<0.3)
    """
    if gdf.empty:
        return gdf

    out = gdf.copy()

    # Area score: calibrated for typical fire sizes
    # 50ha→0.3, 200ha→0.5, 1000ha→0.7, 5000ha→0.9
    if area_col in out.columns:
        area_vals = out[area_col].clip(lower=1.0)
        area_score = (np.log10(area_vals) - np.log10(25)) / (np.log10(5000) - np.log10(25))
        area_score = (area_score * 0.7 + 0.2).clip(0.1, 1.0)
    else:
        area_score = pd.Series(0.4, index=out.index)

    # Duration score: any multi-period detection is a strong signal
    if doy_start_col in out.columns and doy_end_col in out.columns:
        dur = (out[doy_end_col] - out[doy_start_col])
        dur_score = pd.Series(0.0, index=out.index)
        dur_score[dur > 0] = 0.3   # spans at least 2 DOY values
        dur_score[dur >= 8] = 0.5   # spans multiple 8-day windows
        dur_score[dur >= 16] = 0.7  # sustained fire (3+ windows)
    else:
        dur_score = pd.Series(0.0, index=out.index)

    # Patch count bonus: merged events are more reliable
    if "patch_count" in out.columns:
        merge_bonus = pd.Series(0.0, index=out.index)
        merge_bonus[out["patch_count"] >= 2] = 0.1
        merge_bonus[out["patch_count"] >= 4] = 0.2
    else:
        merge_bonus = pd.Series(0.0, index=out.index)

    # QA mode bonus
    if qa_mode_col in out.columns:
        qa_bonus = out[qa_mode_col].map(
            {"strict": 0.1, "standard": 0.0, "permissive": -0.05}
        ).fillna(0.0)
    else:
        qa_bonus = pd.Series(0.0, index=out.index)

    # Composite score: area dominates, duration and merge boost
    score = area_score * 0.5 + dur_score * 0.3 + merge_bonus + qa_bonus

    # Uncertainty bonus/penalty (if Burn Date Uncertainty is available)
    if "burn_uncertainty" in out.columns:
        unc = out["burn_uncertainty"].fillna(5).values.astype(float)
        # Low uncertainty (1-2 days) → bonus +0.1
        # Normal uncertainty (3-5 days) → no change
        # High uncertainty (>5 days) → penalty -0.1
        unc_bonus = np.where(unc <= 2, 0.1, np.where(unc > 5, -0.1, 0.0))
        score = score + unc_bonus

    score = score.clip(0.0, 1.0)
    out["confidence_score"] = score.round(3)
    out["confidence"] = pd.cut(
        score,
        bins=[-0.001, 0.3, 0.55, 1.001],
        labels=["low", "medium", "high"],
    ).astype(str)

    return out


# ── 8. Full production pipeline ──────────────────────────────────────────────

def run_quality_pipeline(
    gdf: gpd.GeoDataFrame,
    min_area_ha: float = 25.0,
    merge_buffer_m: float = 1000.0,
    merge_date_tolerance: int = 5,
    simplify_m: float = 250.0,
    skip_merge: bool = False,
    refine_perimeters_flag: bool = True,
) -> gpd.GeoDataFrame:
    """Run the complete post-vectorization quality pipeline.

    Steps (in order):
      1. repair_geometries          — fix self-intersections
      2. filter_min_area            — drop noise patches
      3. merge_nearby_patches       — merge spatially adjacent same-period burns
      4. refine_perimeters          — smooth raster staircase artifacts
      5. add_duration_fields        — compute duration_days
      6. add_confidence_scores      — assign confidence label
      7. simplify_for_web           — reduce vertex count for tile rendering

    Parameters
    ----------
    gdf : GeoDataFrame
        Input in EPSG:6933 (equal-area metres).
    min_area_ha : float
        Minimum patch area in hectares. Default 25 (= 1 VIIRS pixel).
    merge_buffer_m : float
        Proximity buffer for event merging in metres. Default 1000m.
    merge_date_tolerance : int
        Max calendar date difference to merge neighbouring patches. Default 5 days.
    simplify_m : float
        Simplification tolerance in metres for web output. Default 250m.
    skip_merge : bool
        Skip the merge step (faster, useful for testing). Default False.
    refine_perimeters_flag : bool
        Apply perimeter refinement to smooth raster artifacts. Default True.

    Returns
    -------
    GeoDataFrame in original CRS (EPSG:6933).
    """
    if gdf is None or gdf.empty:
        return gdf

    n0 = len(gdf)
    logger.info("Quality pipeline: %d input patches", n0)

    gdf = repair_geometries(gdf)
    gdf = filter_min_area(gdf, min_area_ha=min_area_ha)

    if not skip_merge:
        gdf = merge_nearby_patches(
            gdf,
            buffer_m=merge_buffer_m,
            date_tolerance_days=merge_date_tolerance,
        )

    if refine_perimeters_flag:
        gdf = refine_perimeters(gdf)

    gdf = add_duration_fields(gdf)
    gdf = add_confidence_scores_intrinsic(gdf)
    gdf = simplify_for_web(gdf, tolerance_m=simplify_m)

    logger.info(
        "Quality pipeline complete: %d → %d events (%.1f%% reduction).",
        n0, len(gdf), (1 - len(gdf) / max(n0, 1)) * 100,
    )
    return gdf
