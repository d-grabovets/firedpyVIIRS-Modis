<div align="center">

# firedpyVIIRS&MODIS

### Advanced Fire Event Delineation System

**From raw satellite pixels to operational fire intelligence — anywhere on Earth**

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![VIIRS VNP64A1](https://img.shields.io/badge/VIIRS-VNP64A1.002-orange.svg)](https://lpdaac.usgs.gov/products/vnp64a1v002/)
[![NASA FIRMS](https://img.shields.io/badge/NASA-FIRMS%20API-red.svg)](https://firms.modaps.eosdis.nasa.gov/)
[![License: AGPL-3.0](https://img.shields.io/badge/VIIRS_License-AGPL--3.0-red.svg)](LICENSE_VIIRS.md)
[![License: MIT](https://img.shields.io/badge/MODIS_License-MIT-yellow.svg)](LICENSE)

*Fork of [earthlab/firedpy](https://github.com/earthlab/firedpy) — extended with VIIRS burned area processing, FIRMS cross-validation, land cover classification, and a real-time web dashboard for operational fire monitoring worldwide.*

**Release: 2026-03-19 21:00 UTC+2**

:ukraine: *Made in Ukraine with love*

---

</div>

## What This Project Does

This system transforms **NASA VIIRS VNP64A1 burned area rasters** (500m monthly composites) into **classified, confidence-scored fire event polygons** — ready for analysis, reporting, and operational decision-making. Works for **any region on Earth** where VNP64A1 data is available.

Compared to using raw VNP64A1 rasters directly, this pipeline adds:
- **Spatial-temporal event grouping** — clusters individual burned pixels into coherent fire events
- **Multi-window union merge** — combines Win08 + Win15 for maximum detection coverage
- **QA bit-mask filtering** — removes water, cloud artifacts, and low-quality observations
- **Confidence scoring** — area + duration + patch count + uncertainty weighting
- **FIRMS cross-validation** — independent active fire confirmation
- **Land cover classification** — fire type identification (agricultural / wildfire / grassfire / peatfire)
- **Perimeter refinement** — morphological smoothing for irregular fire boundaries

### 11-Year Production Results (Ukraine, 2014-2024)

| Year | Raw Patches | Events | Total Area |
|------|------------|--------|------------|
| 2014 | 58,429 | 26,149 | 14,953,895 ha |
| 2015 | 54,078 | 23,990 | 15,969,695 ha |
| 2016 | 38,512 | 16,813 | 9,137,181 ha |
| 2017 | 50,411 | 21,952 | 21,071,475 ha |
| 2018 | 28,828 | 13,477 | 8,222,237 ha |
| 2019 | 49,847 | 21,447 | 15,983,538 ha |
| 2020 | 39,915 | 18,018 | 10,380,973 ha |
| 2021 | 27,963 | 11,910 | 10,592,786 ha |
| 2022 | 22,207 | 10,197 | 8,635,026 ha |
| 2023 | 24,634 | 11,133 | 6,923,100 ha |
| 2024 | 27,599 | 12,093 | 5,879,824 ha |
| **Total** | **422,423** | **187,179** | **127,749,729 ha** |

### The Pipeline

```
VNP64A1 TIFFs (500m burned date rasters, Win08 + Win15)
    |
QA filtering (VNP64A1 bit-mask: land + valid + special condition codes)
    |
Multi-window union merge (earlier DOY for ignition dating)
    |
Vectorization (raster -> polygon with per-patch DOY attribution)
    |
Quality pipeline:
    -> Area filtering (>=25 ha, removes single-pixel noise)
    -> Spatial merging (1km buffer, 5-day calendar date tolerance)
    -> Confidence scoring (area + duration + patch count + uncertainty)
    -> Perimeter refinement (morphological smoothing)
    |
FIRMS cross-validation (NASA active fire hotspots)
    |
Land cover classification (ESA WorldCover 2021)
    |
Export: GeoPackage + GeoJSON + Statistics CSV
    |
Web dashboard (Leaflet) + REST API (FastAPI)
```

### Key Features

| Feature | Description |
|---------|-------------|
| **Sensor-agnostic core** | Supports both MODIS MCD64A1 and VIIRS VNP64A1 via `ProductConfig` |
| **VNP64A1 QA decoder** | 3-mode bit-mask filtering (strict/standard/permissive) with bits 5-7 special condition handling |
| **Union-merge strategy** | Combines Win08 and Win15 temporal windows for maximum fire detection |
| **Calendar date merging** | Correctly handles year-boundary fires (Dec+Jan) using calendar dates, not raw DOY |
| **FIRMS cross-validation** | NASA active fire hotspots confirm events — boosts confidence |
| **Land cover classification** | ESA WorldCover 10m classifies fires as agricultural, wildfire, grassfire, peatfire |
| **Cross-sensor validation** | Compare VIIRS vs MODIS events with IoU, area bias, and temporal metrics |
| **Web dashboard** | Dark-themed Leaflet map with filters by year, sensor, confidence, fire type |
| **REST API** | FastAPI endpoints for programmatic access |
| **Equal-area geometry** | All area calculations in EPSG:6933 — correct hectares regardless of input CRS |

---

## Quick Start

### 1. Clone and Install

```bash
git clone https://github.com/d-grabovets/firedpyVIIRS-Modis.git
cd firedpyVIIRS-Modis
pip install -e .
```

### 2. Set Environment Variables

```bash
# Required: NASA FIRMS API key (free)
# Get yours at: https://firms.modaps.eosdis.nasa.gov/api/area/
export FIRMS_API_KEY="your_key_here"

# Required: path to VNP64A1 burn TIFs
export VIIRS_DATA_ROOT="/path/to/VNP64A1_TIFF"

# Optional: ESA WorldCover raster for land cover classification
export WORLDCOVER_PATH="/path/to/worldcover_6933_500m.tif"
```

### 3. Run Production Pipeline

```bash
# Single year
python scripts/build_burned_area.py \
    --year 2024 \
    --burn-dir /data/VNP64A1/raw/2024 \
    --qa-dir /data/VNP64A1/raw/2024 \
    --qa-mode standard \
    --merge-tolerance 5

# Multi-year with FIRMS and land cover
python scripts/run_production.py --year 2023 --year 2024 \
    --download-firms --classify-landcover
```

### 4. Launch Web Dashboard

```bash
uvicorn scripts.api_server:app --host 0.0.0.0 --port 8000
# Open http://localhost:8000/web/
```

---

## Project Structure

```
firedpyVIIRS-Modis/
+-- firedpy/                        # Core library
|   +-- product_config.py           # Sensor-agnostic MODIS/VIIRS config
|   +-- firms_integration.py        # NASA FIRMS download & cross-validation
|   +-- event_quality.py            # Confidence scoring & quality pipeline
|   +-- landcover_classify.py       # ESA WorldCover fire type classification
|   +-- export_geojson.py           # GeoJSON export for web
|   +-- qa/
|   |   +-- viirs_qa.py             # VNP64A1 QA bit-mask decoder (3 modes)
|   +-- validation/
|   |   +-- cross_sensor.py         # VIIRS vs MODIS event matching
|   +-- data_classes.py             # Data access (Earthaccess, parametrized)
|   +-- model_classes.py            # EventGrid space-time clustering
+-- scripts/
|   +-- build_burned_area.py        # Core VIIRS burned area builder
|   +-- run_production.py           # Production pipeline orchestrator
|   +-- api_server.py               # FastAPI REST API server
|   +-- validate_cross_sensor.py    # Cross-sensor validation CLI
+-- web/
|   +-- index.html                  # Leaflet web dashboard
+-- examples/
|   +-- ukraine_2014/               # Example output: 6,069 events
+-- config/                         # Configuration templates
+-- ARCHITECTURE_PLAN.md            # Technical architecture document
```

---

## Data Sources

| Source | Resolution | Coverage | Usage |
|--------|-----------|----------|-------|
| [VIIRS VNP64A1.002](https://lpdaac.usgs.gov/products/vnp64a1v002/) | 500m, monthly | Global, 2012+ | Primary burned area detection |
| [NASA FIRMS VNP14IMG](https://firms.modaps.eosdis.nasa.gov/) | 375m, near-real-time | Global | Active fire cross-validation |
| [ESA WorldCover 2021](https://esa-worldcover.org/) | 10m | Global | Land cover classification |
| [MODIS MCD64A1.061](https://lpdaac.usgs.gov/products/mcd64a1v061/) | 500m, monthly | Global, 2000+ | Alternative/comparison product |

---

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Service health check |
| `GET /burned/events?year=2024` | Fire events GeoJSON |
| `GET /burned/bbox?year=2024&west=30&east=36&south=49&north=52` | Spatial query |
| `GET /burned/stats?year=2024` | Monthly statistics |
| `GET /burned/summary?year=2024` | Annual summary |
| `GET /docs` | Interactive Swagger UI |

---

## What Was Improved Over Original firedpy

| # | Improvement | Impact |
|---|-------------|--------|
| 1 | **VIIRS VNP64A1.002 support** | Higher-resolution burned area (500m grid, 375m sensor) |
| 2 | **ProductConfig architecture** | Sensor-agnostic — switch MODIS/VIIRS via `--product` flag |
| 3 | **QA bit-mask filtering** | 3 modes with bits 5-7 special condition codes |
| 4 | **Multi-window union merge** | Win08+Win15 combined for maximum detection |
| 5 | **Calendar date merging** | Fixes year-boundary bug (Dec+Jan fires correctly merged) |
| 6 | **Equal-area CRS** | All geometry operations in EPSG:6933, not geographic degrees |
| 7 | **FIRMS integration** | Active fire hotspot cross-validation |
| 8 | **Land cover classification** | ESA WorldCover fire type identification |
| 9 | **Confidence recalibration** | Balanced 13/43/44% (high/med/low) with uncertainty weighting |
| 10 | **Cross-sensor validation** | VIIRS vs MODIS event matching with IoU and area bias |
| 11 | **Web dashboard** | Leaflet map with sensor/year/confidence/type filters |
| 12 | **REST API** | FastAPI for programmatic access |

---

## Licensing

This repository uses a **dual license** model:

| Component | License | Commercial Use |
|-----------|---------|----------------|
| Original firedpy code (MODIS) | [MIT License](LICENSE) | Allowed |
| VIIRS extension (new modules) | [AGPL-3.0](LICENSE_VIIRS.md) | Must open-source your code, or buy a commercial license |

The VIIRS extension is licensed under **AGPL-3.0** — free to use, but you must open-source any modifications and derivative works. Companies that want to keep their code proprietary can purchase a **Commercial License** — see [LICENSE_VIIRS.md](LICENSE_VIIRS.md) for details.

---

## Credits

- **Original firedpy**: [Earth Lab, University of Colorado Boulder](https://github.com/earthlab/firedpy)
- **VIIRS Extension & Global Adaptation**: [Dmytro Grabovets](https://github.com/d-grabovets)
- **NASA FIRMS**: Fire Information for Resource Management System
- **ESA WorldCover**: European Space Agency land cover product

---

<div align="center">

:ukraine: *Developed in Ukraine with love* :ukraine:

*For firefighters, researchers, and anyone who cares about our planet*

</div>
