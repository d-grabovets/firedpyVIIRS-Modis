#!/usr/bin/env python3
"""
Cross-sensor validation: compare VIIRS vs MODIS burned area events.

Usage:
    python scripts/validate_cross_sensor.py \
        --viirs output/viirs_2023_burned_events.gpkg \
        --modis output/modis_2023_burned_events.gpkg \
        --output-dir output/validation/

Outputs:
    validation/matched_pairs.gpkg     — matched event pairs with IoU + Δdate
    validation/viirs_only.gpkg        — VIIRS events with no MODIS match
    validation/modis_only.gpkg        — MODIS events with no VIIRS match
    validation/report.txt             — summary statistics
    validation/report.json            — machine-readable report
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import geopandas as gpd

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from firedpy.validation.cross_sensor import match_events, validation_report, print_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    ap = argparse.ArgumentParser(
        description="Cross-sensor validation: VIIRS vs MODIS burned area events"
    )
    ap.add_argument("--viirs", required=True, help="Path to VIIRS events GeoPackage")
    ap.add_argument("--modis", required=True, help="Path to MODIS events GeoPackage")
    ap.add_argument("--output-dir", default="output/validation", help="Output directory")
    ap.add_argument("--iou-threshold", type=float, default=0.1, help="Minimum IoU for match (default: 0.1)")
    ap.add_argument("--date-tolerance", type=int, default=10, help="Max days between ignition dates (default: 10)")
    ap.add_argument("--date-col", default="ig_date", help="Column name for ignition date")
    args = ap.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    logger.info("Loading VIIRS events: %s", args.viirs)
    viirs = gpd.read_file(args.viirs)
    logger.info("  → %d events", len(viirs))

    logger.info("Loading MODIS events: %s", args.modis)
    modis = gpd.read_file(args.modis)
    logger.info("  → %d events", len(modis))

    # Run matching
    logger.info("Matching events (IoU >= %.2f, Δdate <= %d days)...",
                args.iou_threshold, args.date_tolerance)

    matched, viirs_only, modis_only = match_events(
        viirs, modis,
        iou_threshold=args.iou_threshold,
        date_tolerance_days=args.date_tolerance,
        date_col=args.date_col,
    )

    # Generate report
    report = validation_report(matched, viirs_only, modis_only, len(viirs), len(modis))
    report_text = print_report(report)

    # Print to console
    print()
    print(report_text)
    print()

    # Save outputs
    if len(matched) > 0:
        matched.to_file(out_dir / "matched_pairs.gpkg", driver="GPKG")
        logger.info("Saved: %s (%d pairs)", out_dir / "matched_pairs.gpkg", len(matched))

    if len(viirs_only) > 0:
        viirs_only.to_file(out_dir / "viirs_only.gpkg", driver="GPKG")
        logger.info("Saved: %s (%d events)", out_dir / "viirs_only.gpkg", len(viirs_only))

    if len(modis_only) > 0:
        modis_only.to_file(out_dir / "modis_only.gpkg", driver="GPKG")
        logger.info("Saved: %s (%d events)", out_dir / "modis_only.gpkg", len(modis_only))

    # Save report
    with open(out_dir / "report.txt", "w", encoding="utf-8") as f:
        f.write(report_text)

    with open(out_dir / "report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info("Saved reports to %s", out_dir)
    logger.info("Done!")


if __name__ == "__main__":
    main()
