# firedpyVIIRS&MODIS — Architectural Plan v2.0

## Opus Architect → Sonnet Developer

**Date:** 2026-03-19
**Status:** APPROVED FOR IMPLEMENTATION

---

## Executive Summary

The codebase is a **hybrid system with structural gaps**. Two parallel pipelines exist:

| Pipeline | Files | Status |
|----------|-------|--------|
| **Old (MODIS-only)** | `firedpy/data_classes.py`, `firedpy/model_classes.py` | Working but hardcoded to MCD64A1.061 |
| **New (VIIRS scripts)** | `scripts/build_burned_area.py` + quality modules | **BLOCKED**: missing `firedpy/qa/viirs_qa.py` |

**Goal:** Unify into a single sensor-agnostic pipeline that supports both MODIS MCD64A1 and VIIRS VNP64A1.

---

## Critical Bugs Found (Fix First)

### B1 — CRITICAL: Missing QA Module
- **File:** `scripts/build_burned_area.py:53` imports `parse_burn_qa` from `firedpy.qa.viirs_qa`
- **Problem:** Module does not exist. QA filtering is completely broken.
- **Fix:** Create `firedpy/qa/__init__.py` + `firedpy/qa/viirs_qa.py` with proper VNP64A1 QA bit decoding

### B2 — HIGH: MODIS Hardcoded in Core
- **File:** `firedpy/data_classes.py:127,433,435,679,691`
- **Problem:** `MCD64A1.061` hardcoded everywhere — regex, URLs, earthaccess queries
- **Fix:** Parametrize with `ProductConfig` (Phase 2)

### B3 — MEDIUM: Temporal Merge Bug at Year Boundary
- **File:** `firedpy/event_quality.py:420` — `merge_date_tolerance=16` uses absolute DOY
- **Problem:** Dec DOY=350, Jan DOY=10 → |350-10|=340 > 16 → fires split incorrectly
- **Fix:** Use calendar dates for merge, not raw DOY

### B4 — MEDIUM: Config Exists but Unused
- **File:** `config/default.yaml` defines VNP64A1 params, but `BurnData` ignores config
- **Fix:** Wire config loading into pipeline (Phase 2)

### B5 — LOW: Wrong Time Divisor
- **File:** `firedpy/model_classes.py:501` — `temporal_param // 30` assumes 30-day periods
- **Problem:** VNP64A1 uses 8-day composites, not 30-day
- **Fix:** Make divisor configurable or derive from product type

---

## Implementation Phases

### Phase 1: FOUNDATION (Priority: CRITICAL)
**Goal:** Make the existing VIIRS pipeline actually work correctly

#### TASK-1.1: Create `firedpy/qa/viirs_qa.py` ★★★
Create the missing QA module with proper VNP64A1 bit-mask parsing.

```
firedpy/qa/
├── __init__.py          # NEW
└── viirs_qa.py          # NEW — ~120 lines
```

**VNP64A1 QA Bit Structure:**
```
Bit 0: 0=water, 1=land
Bit 1: 0=no data, 1=valid
Bit 2: 0=normal period, 1=shortened mapping period
Bit 3: 0=direct detection, 1=relabeled contextually
Bit 4: unused (always 0)
Bits 5-7: special condition code (0-5)
  0 = normal
  1 = too few cloud-free obs
  2 = too few training observations
  3 = no training from nearby tiles
  4 = training in a different month
  5 = persistent hotspot
```

**Three QA modes:**
```python
def parse_burn_qa(qa_array: np.ndarray, mode: str = "standard") -> np.ndarray:
    """
    Returns boolean mask: True = valid pixel.

    Modes:
      strict:     bit0=1 AND bit1=1 AND bit2=0 AND special_code==0
      standard:   bit0=1 AND bit1=1 AND special_code < 3
      permissive: bit0=1 AND bit1=1
    """
    land     = (qa_array & 0b00000001).astype(bool)      # bit 0
    valid    = (qa_array & 0b00000010).astype(bool)       # bit 1
    normal   = ~((qa_array & 0b00000100).astype(bool))    # bit 2 = 0
    special  = (qa_array >> 5) & 0b111                    # bits 5-7

    if mode == "strict":
        return land & valid & normal & (special == 0)
    elif mode == "standard":
        return land & valid & (special < 3)
    else:  # permissive
        return land & valid
```

**Acceptance criteria:**
- [ ] `parse_burn_qa()` returns correct mask for all 3 modes
- [ ] Unit tests with synthetic QA arrays covering all bit combinations
- [ ] `build_burned_area.py` imports and uses it without errors

---

#### TASK-1.2: Fix Year-Boundary Temporal Merge ★★★
**File:** `firedpy/event_quality.py` → `merge_nearby_patches()`

**Current (broken):**
```python
joined["doy_diff"] = (joined["doy_start"] - joined["doy_start_r"]).abs()
pairs = joined[joined["doy_diff"] <= date_tolerance_days]
```

**Fix:** Use `ig_date` (calendar date) instead of DOY:
```python
# Convert DOY columns to actual dates for comparison
joined["date_diff"] = (
    pd.to_datetime(joined["ig_date"]) - pd.to_datetime(joined["ig_date_r"])
).dt.days.abs()
pairs = joined[joined["date_diff"] <= date_tolerance_days]
```

**Acceptance criteria:**
- [ ] Fires in Dec 2023 + Jan 2024 correctly merge if within tolerance
- [ ] No regression for same-year merges
- [ ] Test case: DOY 360 (year=2023) + DOY 5 (year=2024) → merged if tolerance >= 11

---

#### TASK-1.3: Reduce Default merge_tolerance from 16 to 5 ★★
**File:** `firedpy/event_quality.py` → `run_quality_pipeline()`

**Rationale:** VNP64A1 is a monthly product. 16-day tolerance causes unrelated fires in adjacent months to merge. Research recommends ≤5 days for VNP64A1.

**Change:**
```python
def run_quality_pipeline(
    gdf,
    min_area_ha=25.0,
    merge_buffer_m=1000.0,
    merge_date_tolerance=5,       # ← was 16
    simplify_m=250.0,
):
```

Also update CLI default in `scripts/build_burned_area.py`.

---

### Phase 2: SENSOR-AGNOSTIC CORE (Priority: HIGH)
**Goal:** Parametrize the core to support both MODIS and VIIRS

#### TASK-2.1: Create `ProductConfig` Data Class ★★★
**New file:** `firedpy/product_config.py` (~80 lines)

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class ProductConfig:
    """Sensor-agnostic product definition."""
    short_name: str           # "MCD64A1" or "VNP64A1"
    version: str              # "061" or "002"
    sensor: str               # "MODIS" or "VIIRS"
    pixel_size_m: float       # 463.31 (both products on same grid)
    record_start_year: int    # 2000 (MODIS) or 2012 (VIIRS)
    sds_burn_date: str        # "Burn Date" (same for both)
    sds_qa: str               # "QA" (same for both)
    sds_uncertainty: str      # "Burn Date Uncertainty"
    grid_name: str            # "MOD_Grid_Monthly_500m_BA" (same for both!)
    special_values: dict      # {0: "unburned", -1: "unmapped", -2: "water"}
    lp_daac_catalog: str      # LP DAAC subfolder
    file_regex: str           # Regex for filename parsing

MODIS_MCD64A1 = ProductConfig(
    short_name="MCD64A1",
    version="061",
    sensor="MODIS",
    pixel_size_m=463.31271653,
    record_start_year=2000,
    sds_burn_date="Burn Date",
    sds_qa="QA",
    sds_uncertainty="Burn Date Uncertainty",
    grid_name="MOD_Grid_Monthly_500m_BA",
    special_values={0: "unburned", -1: "unmapped", -2: "water"},
    lp_daac_catalog="MOTA/MCD64A1.061",
    file_regex=r"MCD64A1\.A(\d{4})(\d{3})\.h(\d{2})v(\d{2})\.061\.\d+\.hdf",
)

VIIRS_VNP64A1 = ProductConfig(
    short_name="VNP64A1",
    version="002",
    sensor="VIIRS",
    pixel_size_m=463.31271653,    # Same grid as MODIS!
    record_start_year=2012,
    sds_burn_date="Burn Date",
    sds_qa="QA",
    sds_uncertainty="Burn Date Uncertainty",
    grid_name="MOD_Grid_Monthly_500m_BA",  # Same grid name!
    special_values={0: "unburned", -1: "unmapped", -2: "water"},
    lp_daac_catalog="VIIRS/VNP64A1.002",
    file_regex=r"VNP64A1\.A(\d{4})(\d{3})\.h(\d{2})v(\d{2})\.002\.\d+\.hdf",
)

PRODUCTS = {"MCD64A1": MODIS_MCD64A1, "VNP64A1": VIIRS_VNP64A1}
```

---

#### TASK-2.2: Parametrize `BurnData` in `data_classes.py` ★★★
**Key changes in `firedpy/data_classes.py`:**

1. **`__init__`:** Accept `product: str = "MCD64A1"`, look up `ProductConfig`
2. **`_file_regex`:** Use `config.file_regex` instead of hardcoded string
3. **`_lp_daac_url`:** Construct from `config.lp_daac_catalog`
4. **`_get_granules`:** Use `config.short_name` and `config.version` in earthaccess call
5. **`_write_ncs`:** Select SDS by name (`config.sds_burn_date`) not index `[0]`
6. **`_record_start_year`:** Use `config.record_start_year`

**SDS Selection Fix (critical):**
```python
# BEFORE (fragile):
sds = gdal.Open(sample).GetSubDatasets()[0][0]

# AFTER (robust):
ds = gdal.Open(sample)
for sds_name, sds_desc in ds.GetSubDatasets():
    if config.sds_burn_date in sds_desc:
        burn_sds = sds_name
        break
else:
    raise ValueError(f"SDS '{config.sds_burn_date}' not found in {sample}")
```

---

#### TASK-2.3: Add `--product` CLI Flag ★★
**File:** `firedpy/cli.py`

```python
@click.option(
    "-pr", "--product",
    type=click.Choice(["MCD64A1", "VNP64A1"]),
    default="MCD64A1",
    help="Burned area product: MCD64A1 (MODIS) or VNP64A1 (VIIRS)"
)
```

Pass through to `BurnData(product=product)`.

---

### Phase 3: QA & UNCERTAINTY INTEGRATION (Priority: HIGH)
**Goal:** Use QA and uncertainty layers for better event quality

#### TASK-3.1: QA Filtering in `_write_ncs()` ★★
When converting HDF→NetCDF, apply QA mask:

```python
burn_ds = gdal.Open(burn_sds)
qa_ds = gdal.Open(qa_sds)  # NEW: also open QA SDS

burn_arr = burn_ds.ReadAsArray()
qa_arr = qa_ds.ReadAsArray()

# Apply QA mask
from firedpy.qa.viirs_qa import parse_burn_qa
valid = parse_burn_qa(qa_arr, mode=qa_mode)
burn_arr[~valid] = 0  # Mask invalid pixels as unburned
```

#### TASK-3.2: Uncertainty-Weighted Confidence ★
**File:** `firedpy/event_quality.py`

Add `Burn Date Uncertainty` as weight in confidence scoring:
```python
# Low uncertainty (1-2 days) → bonus +0.1
# High uncertainty (>5 days) → penalty -0.1
uncertainty_bonus = np.where(uncertainty <= 2, 0.1, np.where(uncertainty > 5, -0.1, 0.0))
score += uncertainty_bonus
```

---

### Phase 4: EARTHACCESS DATA PIPELINE (Priority: MEDIUM)
**Goal:** Automatic download of VNP64A1 via NASA Earthaccess

#### TASK-4.1: VNP64A1 Download via Earthaccess ★★
**File:** `firedpy/data_classes.py` → `_get_granules()`

```python
results = earthaccess.search_data(
    short_name=self._config.short_name,  # "VNP64A1"
    version=self._config.version,        # "002"
    temporal=(start_date, end_date),
    bounding_box=(west, south, east, north),
)
files = earthaccess.download(results, local_path=self._data_dir)
```

**Important:** VNP64A1 Version 1 was deactivated July 31, 2025. Must use Version 2 (002).

#### TASK-4.2: Tile Selection for Any Region ★
Create helper to find MODIS sinusoidal tiles for any bounding box:

```python
def bbox_to_modis_tiles(west, south, east, north) -> list[str]:
    """Return list of 'hXXvYY' tile IDs covering the bbox."""
    # Use MODIS sinusoidal grid parameters
    # T = 1111950 m (tile size), 36 cols x 18 rows
```

---

### Phase 5: TRUE EVENT DELINEATION (Priority: MEDIUM)
**Goal:** Use the real FIREDpy EventGrid algorithm instead of buffer+dissolve

#### TASK-5.1: Wire VIIRS Data into EventGrid ★★★
The current VIIRS pipeline uses `buffer(1000).dissolve()` which is a geometric approximation.
The real `model_classes.EventGrid` uses space-time flooding — scientifically correct but needs VIIRS NetCDF input.

**Plan:**
1. `_write_ncs()` now produces NetCDF from VNP64A1 (after Phase 2)
2. `EventGrid` reads those NetCDs — same format, same grid
3. Parameters: `spatial_param=5` (2315m), `temporal_param=5` (VNP64A1 monthly)

**Why this matters:** Buffer+dissolve ignores temporal fire propagation direction. EventGrid tracks which pixels burned first → fire spread reconstruction.

#### TASK-5.2: Optimal Parameters for VIIRS ★
Research recommends:

| Region Type | spatial_param | temporal_param | Source |
|-------------|---------------|----------------|--------|
| Global default | 5 | 5 | This analysis |
| Boreal forests | 7 | 15 | Balch 2020 |
| Agricultural | 3 | 3 | Empirical |
| Tropical | 5 | 5 | Default |

Add `--region-preset` CLI option or auto-detect from land cover.

---

### Phase 6: CROSS-SENSOR VALIDATION (Priority: MEDIUM)
**Goal:** Validate VIIRS events against MODIS events for the overlap period

#### TASK-6.1: Cross-Sensor Matching Module ★★
**New file:** `firedpy/validation/cross_sensor.py` (~200 lines)

```python
def match_events(viirs_gdf, modis_gdf, iou_threshold=0.2, date_tolerance=7):
    """Match VIIRS events to MODIS events by spatial overlap + temporal proximity."""
    # Returns: matched_pairs, viirs_only, modis_only
    # Metrics: IoU, area_bias, delta_date, fragmentation_ratio

def validation_report(matched, viirs_only, modis_only) -> dict:
    """Generate summary statistics for cross-sensor comparison."""
```

#### TASK-6.2: Validation Script ★
```bash
python scripts/validate_cross_sensor.py \
    --viirs-events output/viirs_2023_burned_events.gpkg \
    --modis-events output/modis_2023_burned_events.gpkg \
    --output-dir output/validation/
```

Output: scatter plots, IoU histograms, delta-date distributions, commission/omission maps.

---

### Phase 7: OPERATIONAL ENHANCEMENTS (Priority: LOW)
**Goal:** Production-ready features from fork

#### TASK-7.1: FIRMS Cross-Validation (already done) ✅
`firedpy/firms_integration.py` — working, keep as-is.

#### TASK-7.2: Land Cover Classification (already done) ✅
`firedpy/landcover_classify.py` — working, keep as-is.

#### TASK-7.3: Web Dashboard Improvements ★
- Add MODIS/VIIRS toggle
- Add cross-sensor comparison view
- Add time-series animation

#### TASK-7.4: Scheduled Pipeline ★
- Cron/Task Scheduler for monthly updates
- Auto-download new VNP64A1 tiles when available

---

## Dependency Graph

```
Phase 1 (Foundation)
  ├── TASK-1.1: Create viirs_qa.py ──────────────────┐
  ├── TASK-1.2: Fix year-boundary merge              │
  └── TASK-1.3: Reduce merge tolerance               │
                                                      ▼
Phase 2 (Sensor-Agnostic) ◄──── depends on Phase 1
  ├── TASK-2.1: ProductConfig ───────────────────────┐
  ├── TASK-2.2: Parametrize BurnData ◄── 2.1         │
  └── TASK-2.3: --product CLI flag ◄── 2.2           │
                                                      ▼
Phase 3 (QA Integration) ◄──── depends on 1.1 + 2.2
  ├── TASK-3.1: QA in _write_ncs()
  └── TASK-3.2: Uncertainty-weighted confidence
                    │
Phase 4 (Earthaccess) ◄──── depends on 2.1
  ├── TASK-4.1: VNP64A1 download
  └── TASK-4.2: Tile selection
                    │
Phase 5 (True EventGrid) ◄──── depends on Phase 2+3
  ├── TASK-5.1: Wire VIIRS into EventGrid
  └── TASK-5.2: Optimal parameters
                    │
Phase 6 (Validation) ◄──── depends on Phase 5
  ├── TASK-6.1: Cross-sensor matching
  └── TASK-6.2: Validation script
                    │
Phase 7 (Operational) ◄──── independent, can start anytime
  └── TASK-7.x: Dashboard, scheduling
```

---

## Effort Estimates

| Phase | Tasks | Complexity | Estimate |
|-------|-------|------------|----------|
| **Phase 1** | 3 tasks | Small-Medium | **1-2 days** |
| **Phase 2** | 3 tasks | Medium-Large | **3-5 days** |
| **Phase 3** | 2 tasks | Medium | **2-3 days** |
| **Phase 4** | 2 tasks | Medium | **2-3 days** |
| **Phase 5** | 2 tasks | Large | **5-7 days** |
| **Phase 6** | 2 tasks | Large | **5-7 days** |
| **Phase 7** | 4 tasks | Mixed | **3-5 days** |
| **TOTAL** | 18 tasks | — | **~3-5 weeks** |

---

## Risk Register

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| VNP64A1 HDF4 format differs from docs | Low | High | Test with real downloaded tiles first |
| EventGrid OOM on large regions | Medium | Medium | Dask chunked processing (Phase 5) |
| MODIS↔VIIRS events don't correlate well | Medium | High | Accept as known limitation, document bias |
| earthaccess API changes | Low | Medium | Pin version, add fallback direct download |
| QA bits 5-7 incorrectly parsed | Medium | High | Unit tests with known QA arrays from NASA |

---

## Success Criteria

### MVP (Phase 1-2 complete):
- [ ] `firedpy --product VNP64A1 --country Ukraine --start-year 2023 --end-year 2024` works
- [ ] QA filtering produces visibly fewer false positives than no-QA
- [ ] Output GeoPackage has correct CRS, valid geometries, reasonable event counts

### Full System (Phase 1-6 complete):
- [ ] Both MODIS and VIIRS produce events from same CLI
- [ ] Cross-sensor validation report shows median Δdate ≤ 3 days for matched events
- [ ] Area bias (VIIRS vs MODIS) documented per land cover class
- [ ] >90% of MTBS-verified large fires (>1000 ha) detected by VIIRS pipeline

---

## Notes for Sonnet Developer

1. **Start with Phase 1** — it unblocks everything else
2. **TASK-1.1 is the #1 priority** — without `viirs_qa.py` the entire VIIRS pipeline is broken
3. **Don't touch `model_classes.py` until Phase 5** — it's complex and working for MODIS
4. **Test with real data** — Ukraine tiles h20v03, h20v04, h21v03, h21v04 for 2023-2024
5. **VNP64A1 uses the SAME grid as MCD64A1** — this is the key insight that makes adaptation feasible
6. **Git branch strategy:** one branch per phase, PR to main after tests pass

---

*Architectural plan by Opus. Implementation by Sonnet.*
*🇺🇦 Made in Ukraine with love*
