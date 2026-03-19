"""
FastAPI server for VIIRS burned area data.

Serves pre-computed GeoJSON and GeoPackage files.
No GABAM, no on-demand computation — static file serving with metadata.

Endpoints
---------
GET  /health                          — liveness probe
GET  /burned/events?year=2024         — all fire events GeoJSON
GET  /burned/events?year=2024&month=9 — filtered by month
GET  /burned/stats?year=2024          — monthly statistics JSON
GET  /burned/bbox?year=2024&west=30&east=36&south=49&north=52
                                      — spatial bbox filter
GET  /docs                            — Swagger UI

Run
---
uvicorn scripts.api_server:app --host 0.0.0.0 --port 8000 --reload

Or with gunicorn:
gunicorn scripts.api_server:app -w 4 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8000
"""

import json
import logging
import os
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger("api_server")
logging.basicConfig(level=logging.INFO)

# ── config ────────────────────────────────────────────────────────────────────
OUTPUT_DIR = Path(os.environ.get(
    "VIIRS_OUTPUT_DIR",
    Path(__file__).resolve().parents[1] / "output",
))

app = FastAPI(
    title="firedpyVIIRS — Burned Area API",
    description="VIIRS VNP64A1 burned area events. Global coverage, any region.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Serve web dashboard + output GeoJSON files
WEB_DIR = Path(__file__).resolve().parents[1] / "web"
if WEB_DIR.exists():
    app.mount("/web", StaticFiles(directory=str(WEB_DIR), html=True), name="web")
app.mount("/output", StaticFiles(directory=str(OUTPUT_DIR)), name="output")


# ── cache ─────────────────────────────────────────────────────────────────────
_cache: dict[str, gpd.GeoDataFrame] = {}


def _load_events(year: int) -> gpd.GeoDataFrame:
    key = f"events_{year}"
    if key not in _cache:
        # Prefer quality-filtered events, fall back to raw
        for fname in (
            f"viirs_{year}_burned_events.gpkg",
            f"viirs_{year}_burned_all.gpkg",
        ):
            path = OUTPUT_DIR / fname
            if path.exists():
                logger.info("Loading %s", path)
                gdf = gpd.read_file(path)
                if gdf.crs and gdf.crs.to_epsg() != 4326:
                    gdf = gdf.to_crs("EPSG:4326")
                _cache[key] = gdf
                break
        else:
            raise FileNotFoundError(f"No burned area file found for year {year}")
    return _cache[key]


def _load_stats(year: int) -> pd.DataFrame:
    key = f"stats_{year}"
    if key not in _cache:
        path = OUTPUT_DIR / f"viirs_{year}_stats.csv"
        if not path.exists():
            raise FileNotFoundError(f"Stats file not found: {path}")
        _cache[key] = pd.read_csv(path)
    return _cache[key]


def _gdf_to_geojson(gdf: gpd.GeoDataFrame) -> dict:
    """Convert GeoDataFrame to GeoJSON dict with serializable types."""
    return json.loads(gdf.to_json())


# ── endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "output_dir": str(OUTPUT_DIR)}


@app.get("/burned/events")
def get_events(
    year: int = Query(2024, description="Year (e.g. 2024)"),
    month: Optional[int] = Query(None, ge=1, le=12, description="Filter by month (1-12)"),
    min_area: Optional[float] = Query(None, description="Minimum area in hectares"),
    confidence: Optional[str] = Query(
        None, pattern="^(high|medium|low)$",
        description="Filter by confidence label"
    ),
):
    """Return fire events as GeoJSON.

    Supports filtering by month, minimum area, and confidence level.
    Returns EPSG:4326 (WGS84) coordinates.
    """
    try:
        gdf = _load_events(year)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if month is not None:
        gdf = gdf[gdf["month"] == month]
    if min_area is not None and "area_ha" in gdf.columns:
        gdf = gdf[gdf["area_ha"] >= min_area]
    if confidence is not None and "confidence" in gdf.columns:
        gdf = gdf[gdf["confidence"] == confidence]

    return Response(
        content=gdf.to_json(),
        media_type="application/geo+json",
        headers={"X-Event-Count": str(len(gdf))},
    )


@app.get("/burned/bbox")
def get_events_bbox(
    year: int = Query(2024),
    west:  float = Query(..., description="West longitude"),
    east:  float = Query(..., description="East longitude"),
    south: float = Query(..., description="South latitude"),
    north: float = Query(..., description="North latitude"),
    month: Optional[int] = Query(None, ge=1, le=12),
):
    """Return fire events within a bounding box (WGS84 coordinates)."""
    from shapely.geometry import box as shapely_box

    try:
        gdf = _load_events(year)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    bbox_geom = shapely_box(west, south, east, north)
    gdf = gdf[gdf.geometry.intersects(bbox_geom)]

    if month is not None:
        gdf = gdf[gdf["month"] == month]

    return Response(
        content=gdf.to_json(),
        media_type="application/geo+json",
        headers={"X-Event-Count": str(len(gdf))},
    )


@app.get("/burned/stats")
def get_stats(year: int = Query(2024)):
    """Return monthly burned area statistics as JSON."""
    try:
        df = _load_stats(year)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return JSONResponse(content=df.to_dict(orient="records"))


@app.get("/burned/summary")
def get_summary(year: int = Query(2024)):
    """Return annual summary statistics."""
    try:
        gdf = _load_events(year)
        df  = _load_stats(year)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    summary = {
        "year": year,
        "total_events": len(gdf),
        "total_area_ha": round(float(gdf["area_ha"].sum()), 1) if "area_ha" in gdf.columns else None,
        "months_with_data": sorted(gdf["month"].unique().tolist()) if "month" in gdf.columns else [],
        "confidence_breakdown": (
            gdf["confidence"].value_counts().to_dict()
            if "confidence" in gdf.columns else {}
        ),
        "peak_month": (
            int(df.loc[df["area_ha"].idxmax(), "month"]) if not df.empty else None
        ),
        "source": "VIIRS VNP64A1.002",
    }
    return JSONResponse(content=summary)
