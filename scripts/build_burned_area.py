"""
Build burned area GeoPackages from VNP64A1 TIFF files.

Uses firedpy.qa.viirs_qa QA filtering + firedpy.event_quality pipeline.
No GABAM or external reference data required.

Usage
-----
# Default (reads DATA_ROOT from env or local fallback):
python scripts/build_burned_area.py

# Explicit paths:
python scripts/build_burned_area.py \
    --burn-dir  /data/VNP64A1_TIFF/region_6933/2024 \
    --qa-dir    /data/VNP64A1_TIFF/qa_region_6933/2024 \
    --output    /srv/wildfires/output \
    --year 2024 --qa-mode standard \
    --min-area 25 --merge-buffer 1000

Environment variables (fallback if flags not provided):
  VIIRS_BURN_DIR   — path to burn TIF directory
  VIIRS_QA_DIR     — path to QA TIF directory
  VIIRS_OUTPUT_DIR — output directory

Outputs
-------
  output/viirs_{year}_burned_all.gpkg   — all patches (EPSG:4326)
  output/viirs_{year}_burned_events.gpkg— quality-filtered events
  output/viirs_{year}_burned_events.geojson — web GeoJSON
  output/viirs_{year}_stats.csv          — monthly statistics
  output/map_{year}_burned_all.png       — full-year map
"""

import argparse
import logging
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import shapes
from shapely.geometry import shape

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from firedpy.qa.viirs_qa import parse_burn_qa
from firedpy.event_quality import run_quality_pipeline
from firedpy.export_geojson import to_geojson
from firedpy.firms_integration import (
    cross_validate_with_firms,
    load_firms_local_csv,
)
from firedpy.landcover_classify import classify_events_by_landcover

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("build_burned_area")

WINDOWS = ["Win08", "Win15"]


def _month_doys(year: int) -> dict[int, int]:
    """Return {month: doy_start} for the given year (handles leap years)."""
    return {m: date(year, m, 1).timetuple().tm_yday for m in range(1, 13)}


# ── helpers ──────────────────────────────────────────────────────────────────

def doy_to_date(year: int, doy: int) -> date:
    return date(year, 1, 1) + timedelta(days=int(doy) - 1)


def parse_doy_from_path(p: Path) -> int:
    m = re.search(r"A(\d{4})(\d{3})", p.name)
    return int(m.group(2)) if m else 0


def load_qa_filtered(
    burn_path: Path, qa_path: Path, qa_mode: str
) -> tuple[np.ndarray, object, object]:
    """Read burn TIF, apply QA, return (array, transform, crs)."""
    with rasterio.open(burn_path) as ds:
        burn = ds.read(1).astype(np.int16)
        transform = ds.transform
        crs = ds.crs

    valid = (burn > 0) & (burn <= 366)

    if qa_path.exists():
        with rasterio.open(qa_path) as ds:
            qa = ds.read(1).astype(np.uint8)
        valid = valid & parse_burn_qa(qa, mode=qa_mode)
    else:
        logger.warning("QA file not found: %s  (using burn-only filter)", qa_path.name)

    result = np.zeros_like(burn)
    result[valid] = burn[valid]
    return result, transform, crs


def mosaic_windows(arr08, t08, arr15, t15) -> tuple[np.ndarray, object]:
    """Merge Win08 and Win15 with union-merge strategy.

    Strategy:
      - If only one window has data → use that value
      - If both windows have data → take the EARLIER DOY (min) for ig_date
        and keep both detections (ensures we capture max extent)
      - This union approach captures fires detected in either temporal window
        rather than losing Win15-only detections
    """
    if arr08.shape != arr15.shape:
        # Different extents — crop to common overlap (min rows/cols)
        min_rows = min(arr08.shape[0], arr15.shape[0])
        min_cols = min(arr08.shape[1], arr15.shape[1])
        arr08 = arr08[:min_rows, :min_cols]
        arr15 = arr15[:min_rows, :min_cols]
    # Union: any pixel burned in either window is burned
    # For DOY value: use earlier detection (min non-zero)
    both = (arr08 > 0) & (arr15 > 0)
    merged = np.where(arr08 > 0, arr08, arr15)
    # Where both detected, use earlier DOY for ignition date
    merged[both] = np.minimum(arr08[both], arr15[both])
    return merged, t08


def vectorise_burned(
    burn_array: np.ndarray,
    transform,
    crs,
    year: int,
    month: int,
    start_doy: int,
) -> gpd.GeoDataFrame:
    """Vectorize burned pixels → polygon GeoDataFrame with per-patch DOY."""
    from rasterio.transform import rowcol

    binary = (burn_array > 0).astype(np.uint8)
    if binary.sum() == 0:
        return gpd.GeoDataFrame(
            columns=["geometry", "month", "year", "doy_start", "doy_end",
                     "ig_date", "last_date", "area_ha", "source", "qa_mode"]
        )

    geom_list = [shape(g) for g, _ in shapes(binary, mask=binary, transform=transform)]
    if not geom_list:
        return gpd.GeoDataFrame()

    rows = []
    for geom in geom_list:
        if geom is None or geom.is_empty:
            continue

        minx, miny, maxx, maxy = geom.bounds
        row_min, col_min = rowcol(transform, minx, maxy)
        row_max, col_max = rowcol(transform, maxx, miny)

        row_min = max(0, int(row_min))
        row_max = min(burn_array.shape[0] - 1, int(row_max))
        col_min = max(0, int(col_min))
        col_max = min(burn_array.shape[1] - 1, int(col_max))

        if row_min > row_max or col_min > col_max:
            continue

        patch = burn_array[row_min:row_max + 1, col_min:col_max + 1]
        vals = patch[patch > 0]
        poly_doy_start = int(vals.min()) if len(vals) else start_doy
        poly_doy_end   = int(vals.max()) if len(vals) else start_doy

        rows.append({
            "geometry":  geom,
            "month":     month,
            "year":      year,
            "doy_start": poly_doy_start,
            "doy_end":   poly_doy_end,
            "ig_date":   str(doy_to_date(year, poly_doy_start)),
            "last_date": str(doy_to_date(year, poly_doy_end)),
            "source":    "viirs",
        })

    if not rows:
        return gpd.GeoDataFrame()

    gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs=crs)
    # Compute area in equal-area CRS (EPSG:6933) for correct hectares
    gdf_ea = gdf.to_crs("EPSG:6933")
    gdf["area_ha"] = (gdf_ea.geometry.area / 10_000).round(2)
    return gdf


# ── main ─────────────────────────────────────────────────────────────────────

def main(args: argparse.Namespace) -> None:
    year     = args.year
    qa_mode  = args.qa_mode
    burn_dir = Path(args.burn_dir)
    qa_dir   = Path(args.qa_dir)
    out_dir  = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("Build VIIRS Burned Area  year=%d  qa=%s", year, qa_mode)
    logger.info("burn_dir : %s", burn_dir)
    logger.info("qa_dir   : %s", qa_dir)
    logger.info("output   : %s", out_dir)
    logger.info("=" * 60)

    all_gdfs: list[gpd.GeoDataFrame] = []
    stats_rows: list[dict] = []

    month_doys = _month_doys(year)
    doy_to_month = {v: k for k, v in month_doys.items()}

    for doy, month in sorted(doy_to_month.items()):
        doy_str = f"{doy:03d}"
        logger.info("Processing %d-%02d (DOY %s)...", year, month, doy_str)

        arrays, transform_ref, crs_ref = [], None, None

        for win in WINDOWS:
            bname = f"VNP64monthly.A{year}{doy_str}.{win}.002.burndate.tif"
            qname = f"VNP64monthly.A{year}{doy_str}.{win}.002.ba_qa.tif"
            bpath = burn_dir / win / bname
            qpath = qa_dir   / win / qname

            if not bpath.exists():
                logger.debug("SKIP %s: %s not found", win, bname)
                continue

            arr, transform, crs = load_qa_filtered(bpath, qpath, qa_mode)
            arrays.append(arr)
            transform_ref = transform
            crs_ref = crs

        if not arrays:
            logger.info("  No data for month %d", month)
            continue

        merged = arrays[0]
        if len(arrays) == 2:
            merged, transform_ref = mosaic_windows(
                arrays[0], transform_ref, arrays[1], transform_ref
            )

        px_count = int((merged > 0).sum())
        px_size_deg = abs(transform_ref.a)  # pixel size in degrees if EPSG:4326
        if px_size_deg < 1:
            # CRS is geographic (degrees) — approximate pixel size in metres
            import math
            mid_lat = transform_ref.f - (merged.shape[0] / 2) * abs(transform_ref.e)
            px_size_m = px_size_deg * 111320 * math.cos(math.radians(mid_lat))
        else:
            px_size_m = px_size_deg  # CRS is already in metres
        area_ha = px_count * px_size_m * px_size_m / 10_000
        logger.info("  Burned pixels: %d  (~%.0f ha)", px_count, area_ha)

        gdf = vectorise_burned(merged, transform_ref, crs_ref, year, month, doy)
        gdf["qa_mode"] = qa_mode

        if not gdf.empty:
            all_gdfs.append(gdf)

        stats_rows.append({
            "year": year, "month": month, "doy_start": doy,
            "burned_pixels": px_count, "area_ha": round(area_ha, 1),
            "patches": len(gdf), "qa_mode": qa_mode, "source": "viirs",
        })

    if not all_gdfs:
        logger.error("No burned area data found. Check burn_dir: %s", burn_dir)
        sys.exit(1)

    # ── raw annual GDF ────────────────────────────────────────────────────────
    annual_6933 = gpd.GeoDataFrame(
        pd.concat(all_gdfs, ignore_index=True),
        geometry="geometry",
        crs=crs_ref,
    )
    annual_wgs84 = annual_6933.to_crs("EPSG:4326")

    out_raw = out_dir / f"viirs_{year}_burned_all.gpkg"
    annual_wgs84.to_file(out_raw, driver="GPKG")
    logger.info("Saved %d raw patches -> %s", len(annual_wgs84), out_raw.name)

    # ── quality pipeline ──────────────────────────────────────────────────────
    logger.info("Running quality pipeline (min_area=%.0fha, merge_buffer=%.0fm)...",
                args.min_area, args.merge_buffer)

    events_6933 = run_quality_pipeline(
        annual_6933,
        min_area_ha=args.min_area,
        merge_buffer_m=args.merge_buffer,
        merge_date_tolerance=args.merge_tolerance,
        simplify_m=args.simplify,
    )
    events_wgs84 = events_6933.to_crs("EPSG:4326")

    # ── FIRMS cross-validation (optional) ─────────────────────────────────
    firms_csv = getattr(args, "firms_csv", None)
    if firms_csv and Path(firms_csv).exists():
        logger.info("Loading FIRMS CSV for cross-validation: %s", firms_csv)
        firms_gdf = load_firms_local_csv(Path(firms_csv))
        logger.info("FIRMS hotspots loaded: %d", len(firms_gdf))
        events_wgs84 = cross_validate_with_firms(
            events_wgs84,
            firms_gdf,
            radius_m=1500.0,
            date_tolerance_days=8,
        )
        # Upgrade confidence using FIRMS confirmation
        if "firms_confirmed" in events_wgs84.columns:
            n_confirmed = events_wgs84["firms_confirmed"].sum()
            logger.info("FIRMS confirmed %d/%d events", n_confirmed, len(events_wgs84))
            # Override intrinsic confidence with FIRMS-enhanced confidence
            events_wgs84.loc[
                events_wgs84["firms_confirmed"], "confidence"
            ] = "high"
            events_wgs84.loc[
                events_wgs84["firms_confirmed"], "confidence_score"
            ] = events_wgs84.loc[
                events_wgs84["firms_confirmed"], "confidence_score"
            ].clip(lower=0.75)

    # ── WorldCover land cover classification (optional) ─────────────────
    wc_path = getattr(args, "worldcover", None)
    if wc_path and Path(wc_path).exists():
        logger.info("Classifying events by land cover: %s", wc_path)
        events_wgs84 = classify_events_by_landcover(
            events_wgs84, Path(wc_path), sample_points=50,
        )

    out_events = out_dir / f"viirs_{year}_burned_events.gpkg"
    events_wgs84.to_file(out_events, driver="GPKG")
    logger.info("Saved %d quality events -> %s", len(events_wgs84), out_events.name)

    # ── GeoJSON for web ───────────────────────────────────────────────────────
    out_geojson = out_dir / f"viirs_{year}_burned_events.geojson"
    to_geojson(events_wgs84, out_geojson)
    logger.info("Saved web GeoJSON -> %s", out_geojson.name)

    # ── stats CSV ─────────────────────────────────────────────────────────────
    stats_df = pd.DataFrame(stats_rows)
    out_stats = out_dir / f"viirs_{year}_stats.csv"
    stats_df.to_csv(out_stats, index=False)
    logger.info("Saved monthly stats -> %s", out_stats.name)

    print("\nMonthly summary:")
    print(stats_df[["month", "burned_pixels", "area_ha", "patches"]].to_string(index=False))

    confidence_summary = (
        events_wgs84["confidence"].value_counts().to_dict()
        if "confidence" in events_wgs84.columns else {}
    )
    print(f"\nEvent confidence: {confidence_summary}")
    print(f"Total events: {len(events_wgs84)}")
    print(f"Total area:   {events_wgs84['area_ha'].sum():,.0f} ha")

    # ── map ───────────────────────────────────────────────────────────────────
    _make_map(
        annual_wgs84,
        f"VIIRS VNP64A1 — Burned Area {year} (QA: {qa_mode})",
        out_dir / f"map_{year}_burned_all.png",
        color_by="month",
    )
    logger.info("Done.")


def _make_map(gdf: gpd.GeoDataFrame, title: str, out_path: Path, color_by: str = "month"):
    if gdf is None or gdf.empty:
        return
    fig, ax = plt.subplots(figsize=(14, 10), facecolor="#1a1a2e")
    ax.set_facecolor("#0d1117")
    gdf.plot(
        ax=ax, column=color_by, cmap="YlOrRd",
        linewidth=0, alpha=0.8, legend=True,
        legend_kwds={"label": color_by, "shrink": 0.5},
    )
    ax.set_title(title, color="white", fontsize=13, fontweight="bold", pad=10)
    ax.set_xlabel("Longitude", color="white")
    ax.set_ylabel("Latitude", color="white")
    ax.tick_params(colors="white")
    for spine in ax.spines.values():
        spine.set_edgecolor("#444")
    ax.text(0.98, 0.02,
            f"Events: {len(gdf):,}   Area: {gdf['area_ha'].sum()/1000:.0f}k ha",
            transform=ax.transAxes, fontsize=9, color="white",
            ha="right", va="bottom",
            bbox=dict(boxstyle="round", facecolor="#1a1a2e", alpha=0.7, edgecolor="#444"))
    fig.savefig(out_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    logger.info("Map saved -> %s", out_path.name)


# ── CLI ───────────────────────────────────────────────────────────────────────

_VIIRS_BASE = (
    os.environ.get("VIIRS_DATA_ROOT", "VNP64A1_TIFF")
)


def _default_burn_dir(year: int = 2024) -> str:
    env = os.environ.get("VIIRS_BURN_DIR")
    if env:
        return env
    return str(Path(_VIIRS_BASE) / f"burn_6933/{year}")


def _default_qa_dir(year: int = 2024) -> str:
    env = os.environ.get("VIIRS_QA_DIR")
    if env:
        return env
    return str(Path(_VIIRS_BASE) / f"qa_6933/{year}")


def _default_output() -> str:
    env = os.environ.get("VIIRS_OUTPUT_DIR")
    if env:
        return env
    return str(Path(__file__).resolve().parents[1] / "output")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build VIIRS burned area from VNP64A1 TIFFs."
    )
    # Parse year first so defaults can use it
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--year", type=int, default=2024)
    pre_args, _ = pre.parse_known_args()
    _year = pre_args.year

    parser.add_argument("--burn-dir",  default=_default_burn_dir(_year),
                        help="Directory with burndate TIF files (subdirs Win08/Win15).")
    parser.add_argument("--qa-dir",    default=_default_qa_dir(_year),
                        help="Directory with ba_qa TIF files (subdirs Win08/Win15).")
    parser.add_argument("--output",    default=_default_output(),
                        help="Output directory.")
    parser.add_argument("--year",      type=int, default=2024,
                        help="Year to process. Default: 2024.")
    parser.add_argument("--qa-mode",   default="standard",
                        choices=["standard", "strict", "permissive"],
                        help="VIIRS QA filter mode. Default: standard.")
    parser.add_argument("--min-area",  type=float, default=25.0,
                        help="Minimum patch area in ha (default 25 = 1 VIIRS pixel).")
    parser.add_argument("--merge-buffer", type=float, default=1000.0,
                        help="Buffer in metres for merging nearby patches (default 1000).")
    parser.add_argument("--merge-tolerance", type=int, default=16,
                        help="Max DOY difference for merging patches (default 16 days).")
    parser.add_argument("--simplify",  type=float, default=250.0,
                        help="Simplification tolerance in metres for web GeoJSON (default 250).")
    parser.add_argument("--firms-csv", type=str, default=None,
                        help="Path to FIRMS CSV file for cross-validation. "
                             "Adds firms_confirmed, firms_count, firms_max_frp columns.")
    parser.add_argument("--worldcover", type=str, default=None,
                        help="Path to ESA WorldCover raster (EPSG:6933 or EPSG:4326). "
                             "Adds land_cover_type, fire_type, seasonal_match columns.")
    parser.add_argument("--product", choices=["MCD64A1", "VNP64A1"], default="VNP64A1",
                        help="Burned area product (default: VNP64A1)")
    args = parser.parse_args()
    main(args)
