"""
Production pipeline runner for VIIRS burned area processing.

This script orchestrates the complete production workflow:
  1. Download latest FIRMS hotspots (near-real-time)
  2. Build burned area events from VNP64A1 TIFs
  3. Cross-validate with FIRMS
  4. Classify by land cover
  5. Export GeoJSON + stats for web display
  6. Serve via FastAPI (optional)

Usage
-----
# Full annual rebuild:
python scripts/run_production.py --year 2024

# With all enrichments:
python scripts/run_production.py --year 2024 --download-firms --classify-landcover

# Quick update (skip FIRMS download, use cached):
python scripts/run_production.py --year 2024 --use-cached-firms

# Multi-year:
python scripts/run_production.py --year 2023 --year 2024

Environment variables
---------------------
  FIRMS_API_KEY    — NASA FIRMS API key (required for --download-firms)
  VIIRS_BURN_DIR   — override burn TIF directory
  VIIRS_QA_DIR     — override QA TIF directory
  VIIRS_OUTPUT_DIR — override output directory
  WORLDCOVER_PATH  — path to ESA WorldCover raster
"""

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("production")

# Defaults
_BASE = Path(__file__).resolve().parents[1]
_SCRIPTS = _BASE / "scripts"
_OUTPUT = Path(os.environ.get("VIIRS_OUTPUT_DIR", _BASE / "output"))
_FIRMS_CACHE = _OUTPUT / "firms_cache"
_WORLDCOVER = os.environ.get(
    "WORLDCOVER_PATH",
    "worldcover_6933_500m.tif"  # Override via WORLDCOVER_PATH env var,
)


def download_firms(api_key: str, year: int) -> Path:
    """Download full year of FIRMS VIIRS hotspots."""
    sys.path.insert(0, str(_BASE))
    from firedpy.firms_integration import download_firms_archive_chunked

    logger.info("Downloading FIRMS VIIRS SNPP for %d...", year)
    _FIRMS_CACHE.mkdir(parents=True, exist_ok=True)

    gdf = download_firms_archive_chunked(
        api_key=api_key,
        year=year,
        source="VIIRS_SNPP_SP",
        cache_dir=_FIRMS_CACHE,
        chunk_days=10,
    )

    csv_path = _FIRMS_CACHE / f"firms_{year}_all.csv"
    if not gdf.empty:
        gdf.drop(columns="geometry", errors="ignore").to_csv(csv_path, index=False)
        logger.info("FIRMS %d: %d hotspots → %s", year, len(gdf), csv_path.name)
    else:
        logger.warning("FIRMS %d: no hotspots downloaded!", year)
        csv_path = None

    return csv_path


def build_year(
    year: int,
    firms_csv: Path | None = None,
    worldcover: str | None = None,
    qa_mode: str = "standard",
    min_area: float = 25.0,
    merge_buffer: float = 1000.0,
) -> None:
    """Run build_burned_area.py for a given year."""
    cmd = [
        sys.executable,
        str(_SCRIPTS / "build_burned_area.py"),
        "--year", str(year),
        "--qa-mode", qa_mode,
        "--min-area", str(min_area),
        "--merge-buffer", str(merge_buffer),
    ]
    if firms_csv and firms_csv.exists():
        cmd.extend(["--firms-csv", str(firms_csv)])
    if worldcover and Path(worldcover).exists():
        cmd.extend(["--worldcover", str(worldcover)])

    logger.info("Building burned area for %d...", year)
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    result = subprocess.run(cmd, env=env, capture_output=False)
    if result.returncode != 0:
        logger.error("Build failed for year %d (exit code %d)", year, result.returncode)
        sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser(description="firedpyVIIRS production pipeline")
    parser.add_argument("--year", type=int, action="append", required=True,
                        help="Year(s) to process (can be specified multiple times).")
    parser.add_argument("--download-firms", action="store_true",
                        help="Download fresh FIRMS data from NASA API.")
    parser.add_argument("--use-cached-firms", action="store_true",
                        help="Use previously cached FIRMS CSV files.")
    parser.add_argument("--classify-landcover", action="store_true",
                        help="Add WorldCover land cover classification.")
    parser.add_argument("--qa-mode", default="standard",
                        choices=["standard", "strict", "permissive"])
    parser.add_argument("--min-area", type=float, default=25.0)
    parser.add_argument("--merge-buffer", type=float, default=1000.0)

    args = parser.parse_args()
    years = sorted(set(args.year))

    api_key = os.environ.get("FIRMS_API_KEY", "")
    if not api_key:
        logger.error("Set FIRMS_API_KEY env variable. Get key at https://firms.modaps.eosdis.nasa.gov/api/area/")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("firedpyVIIRS Production Pipeline")
    logger.info("Years: %s", years)
    logger.info("FIRMS: %s", "download" if args.download_firms else
                "cached" if args.use_cached_firms else "none")
    logger.info("WorldCover: %s", "yes" if args.classify_landcover else "no")
    logger.info("=" * 60)

    start_time = datetime.now()

    for year in years:
        # FIRMS
        firms_csv = None
        if args.download_firms:
            firms_csv = download_firms(api_key, year)
        elif args.use_cached_firms:
            cached = _FIRMS_CACHE / f"firms_{year}_all.csv"
            if cached.exists():
                firms_csv = cached
                logger.info("Using cached FIRMS: %s", cached)
            else:
                logger.warning("No cached FIRMS for %d, skipping cross-validation", year)

        # WorldCover
        worldcover = _WORLDCOVER if args.classify_landcover else None

        # Build
        build_year(
            year=year,
            firms_csv=firms_csv,
            worldcover=worldcover,
            qa_mode=args.qa_mode,
            min_area=args.min_area,
            merge_buffer=args.merge_buffer,
        )

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info("Production pipeline complete in %.0fs", elapsed)
    logger.info("Output: %s", _OUTPUT)
    logger.info("=" * 60)

    # List outputs
    for year in years:
        events = _OUTPUT / f"viirs_{year}_burned_events.gpkg"
        geojson = _OUTPUT / f"viirs_{year}_burned_events.geojson"
        stats = _OUTPUT / f"viirs_{year}_stats.csv"
        for f in [events, geojson, stats]:
            if f.exists():
                size_mb = f.stat().st_size / 1024 / 1024
                logger.info("  %s (%.1f MB)", f.name, size_mb)


if __name__ == "__main__":
    main()
