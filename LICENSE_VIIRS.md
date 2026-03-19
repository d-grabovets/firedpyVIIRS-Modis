# License — VIIRS Extension

## AGPL-3.0 + Commercial License Exception

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

## Open-Source License: AGPL-3.0

The VIIRS extension is licensed under the **GNU Affero General Public License v3.0** (AGPL-3.0).

This means you are free to:
- **Use** — run the software for any purpose
- **Study** — read and modify the source code
- **Share** — redistribute copies
- **Improve** — distribute modified versions

Under the following conditions:
- **Source disclosure** — if you modify and deploy the software (including as a network service), you **must** make the complete source code of your version available under AGPL-3.0
- **Same license** — derivative works must be licensed under AGPL-3.0
- **Attribution** — you must retain copyright notices and attribution

Full AGPL-3.0 text: https://www.gnu.org/licenses/agpl-3.0.en.html

## Commercial License Exception

Companies and commercial organizations that **do not wish to comply with AGPL-3.0 requirements** (i.e., do not want to open-source their own code) may obtain a separate **Commercial License**.

The Commercial License grants:
- Right to use the VIIRS extension in proprietary/closed-source products
- Right to integrate into commercial services without source disclosure
- Right to create derivative works without AGPL-3.0 obligations
- Priority support and consultation

### Who needs a Commercial License?

- For-profit companies integrating this code into closed-source products or services
- Insurance, real estate, or financial companies using it for risk assessment
- SaaS platforms offering fire analytics as a paid service
- Any use where AGPL-3.0 source disclosure is not acceptable

### Who does NOT need a Commercial License?

- Academic and scientific researchers
- Universities and educational institutions
- Non-profit humanitarian organizations (disaster response, civil protection)
- Open-source projects that comply with AGPL-3.0
- Personal non-commercial use

### Contact for Commercial Licensing

**Dmytro Grabovets** — [GitHub](https://github.com/d-grabovets)

## Attribution

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
