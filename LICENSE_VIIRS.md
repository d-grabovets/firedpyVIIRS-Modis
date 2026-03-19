# License — VIIRS Extension (Non-Commercial, Research Only)

**Effective date:** 2026-03-19

This license applies to all VIIRS-specific code and modules added to this repository
beyond the original [earthlab/firedpy](https://github.com/earthlab/firedpy) codebase.

## Covered Files

All files **not present** in the upstream earthlab/firedpy repository, including but not limited to:

- `firedpy/qa/viirs_qa.py` — VIIRS QA bit-mask decoder
- `firedpy/product_config.py` — Sensor-agnostic product configuration
- `firedpy/firms_integration.py` — NASA FIRMS cross-validation
- `firedpy/landcover_classify.py` — ESA WorldCover classification
- `firedpy/event_quality.py` — Confidence scoring and quality pipeline
- `firedpy/export_geojson.py` — GeoJSON export
- `firedpy/validation/` — Cross-sensor validation module
- `scripts/build_burned_area.py` — VIIRS burned area builder
- `scripts/run_production.py` — Production pipeline
- `scripts/api_server.py` — FastAPI REST API
- `scripts/validate_cross_sensor.py` — Validation CLI
- `web/` — Web dashboard
- `config/` — Configuration files
- `ARCHITECTURE_PLAN.md` — Technical documentation

## Terms

### Permitted Uses

1. **Academic and scientific research** — universities, research institutes,
   government research agencies, individual researchers
2. **Education** — classroom use, student projects, thesis/dissertation work
3. **Non-profit humanitarian work** — disaster response, fire monitoring for
   emergency services (e.g., DSNS, CERT, civil protection agencies)
4. **Personal non-commercial use** — hobbyists, open-source contributors

### Prohibited Uses

1. **Commercial use by for-profit companies** — including but not limited to:
   selling derived products, integrating into commercial software or services,
   using for commercial consulting or analytics
2. **Use by insurance, real estate, or financial companies** for risk assessment
   or pricing without a separate commercial license
3. **Reselling or redistributing** as part of a paid product or service

### Commercial Licensing

Companies and commercial organizations wishing to use the VIIRS extension
must obtain a separate commercial license. Contact:

- **Dmytro Grabovets** — [GitHub](https://github.com/d-grabovets)

### Attribution

All uses (commercial or non-commercial) must include attribution:

```
VIIRS Fire Event Delineation by Dmytro Grabovets (d-grabovets/firedpyVIIRS-Modis)
Based on firedpy by Earth Lab, University of Colorado Boulder
```

## Original firedpy Code

The original firedpy code (files present in upstream earthlab/firedpy) remains
under the **MIT License** as specified in the `LICENSE` file. This dual-license
structure does not affect the original MIT-licensed code.

## Disclaimer

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED. IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY ARISING FROM THE USE OF THE SOFTWARE.

---

*Made in Ukraine with love*
