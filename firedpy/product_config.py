"""
Sensor-agnostic product configuration for burned area products.

Supports MODIS MCD64A1 and VIIRS VNP64A1 on the same MODIS sinusoidal grid.
Both products share: 500m grid, MOD_Grid_Monthly_500m_BA, same SDS names.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ProductConfig:
    """Burned area product definition — sensor-agnostic."""

    short_name: str            # "MCD64A1" or "VNP64A1"
    version: str               # "061" or "002"
    sensor: str                # "MODIS" or "VIIRS"
    pixel_size_m: float        # 463.31271653 (both on same grid!)
    record_start_year: int     # 2000 (MODIS) or 2012 (VIIRS)
    sds_burn_date: str         # "Burn Date"
    sds_qa: str                # "QA"
    sds_uncertainty: str       # "Burn Date Uncertainty"
    sds_first_day: str         # "First Day"
    sds_last_day: str          # "Last Day"
    grid_name: str             # "MOD_Grid_Monthly_500m_BA"
    special_values: dict       # {0: "unburned", -1: "unmapped", -2: "water"}
    lp_daac_catalog: str       # subfolder on LP DAAC
    file_regex: str            # regex for parsing filenames
    tiles_per_side: int        # 2400 (pixels per tile side)
    tile_size_m: float         # 1111950.0 (T parameter)

    @property
    def record_end_year(self) -> int:
        """MODIS planned decommission ~2026, VIIRS ongoing."""
        if self.sensor == "MODIS":
            return 2026
        return 2099  # VIIRS — ongoing

    def earthaccess_kwargs(self, start_date: str, end_date: str,
                           bbox: tuple = None) -> dict:
        """Return kwargs for earthaccess.search_data()."""
        kw = {
            "short_name": self.short_name,
            "version": self.version,
            "temporal": (start_date, end_date),
        }
        if bbox:
            kw["bounding_box"] = bbox
        return kw

    def sds_path(self, sds_name: str) -> str:
        """Build HDF4 EOS subdataset path pattern."""
        return f'HDF4_EOS:EOS_GRID:"{{filepath}}":{self.grid_name}:"{sds_name}"'


# ── Pre-configured products ──────────────────────────────────────────────

MODIS_MCD64A1 = ProductConfig(
    short_name="MCD64A1",
    version="061",
    sensor="MODIS",
    pixel_size_m=463.31271653,
    record_start_year=2000,
    sds_burn_date="Burn Date",
    sds_qa="QA",
    sds_uncertainty="Burn Date Uncertainty",
    sds_first_day="First Day",
    sds_last_day="Last Day",
    grid_name="MOD_Grid_Monthly_500m_BA",
    special_values={0: "unburned", -1: "unmapped", -2: "water"},
    lp_daac_catalog="MOTA/MCD64A1.061",
    file_regex=r"MCD64A1\.A(\d{4})(\d{3})\.h(\d{2})v(\d{2})\.061\.\d+\.hdf",
    tiles_per_side=2400,
    tile_size_m=1111950.0,
)

VIIRS_VNP64A1 = ProductConfig(
    short_name="VNP64A1",
    version="002",
    sensor="VIIRS",
    pixel_size_m=463.31271653,    # Same MODIS grid!
    record_start_year=2012,
    sds_burn_date="Burn Date",
    sds_qa="QA",
    sds_uncertainty="Burn Date Uncertainty",
    sds_first_day="First Day",
    sds_last_day="Last Day",
    grid_name="MOD_Grid_Monthly_500m_BA",  # Same as MODIS
    special_values={0: "unburned", -1: "unmapped", -2: "water"},
    lp_daac_catalog="VIIRS/VNP64A1.002",
    file_regex=r"VNP64A1\.A(\d{4})(\d{3})\.h(\d{2})v(\d{2})\.002\.\d+\.hdf",
    tiles_per_side=2400,
    tile_size_m=1111950.0,
)

# Registry for CLI lookup
PRODUCTS: dict[str, ProductConfig] = {
    "MCD64A1": MODIS_MCD64A1,
    "VNP64A1": VIIRS_VNP64A1,
}


def get_product(name: str) -> ProductConfig:
    """Look up product config by short name. Raises KeyError if not found."""
    name = name.upper()
    if name not in PRODUCTS:
        available = ", ".join(PRODUCTS.keys())
        raise KeyError(f"Unknown product '{name}'. Available: {available}")
    return PRODUCTS[name]


def bbox_to_modis_tiles(west: float, south: float, east: float, north: float) -> list[str]:
    """
    Return list of MODIS sinusoidal tile IDs ('hXXvYY') covering a WGS84 bbox.

    The MODIS sinusoidal grid has 36 horizontal x 18 vertical tiles.
    Each tile covers ~10° latitude x ~10° longitude at the equator.

    Parameters
    ----------
    west, south, east, north : WGS84 coordinates in degrees

    Returns
    -------
    List of tile IDs like ['h20v03', 'h20v04', 'h21v03', 'h21v04']
    """
    import math

    EARTH_RADIUS = 6371007.181  # metres (MODIS sphere)
    TILE_SIZE = 1111950.0       # metres per tile
    N_TILES_H = 36
    N_TILES_V = 18

    # Total grid extent in metres
    x_min_global = -N_TILES_H / 2 * TILE_SIZE   # -20015109.354
    y_max_global = N_TILES_V / 2 * TILE_SIZE     #  10007554.677

    def lonlat_to_sinusoidal(lon, lat):
        """Convert WGS84 lon/lat to MODIS sinusoidal x/y (metres)."""
        lat_rad = math.radians(lat)
        lon_rad = math.radians(lon)
        x = EARTH_RADIUS * lon_rad * math.cos(lat_rad)
        y = EARTH_RADIUS * lat_rad
        return x, y

    # Convert bbox corners to sinusoidal
    corners = [
        (west, south), (west, north),
        (east, south), (east, north),
        ((west + east) / 2, south), ((west + east) / 2, north),  # midpoints for curvature
    ]

    xs, ys = [], []
    for lon, lat in corners:
        x, y = lonlat_to_sinusoidal(lon, lat)
        xs.append(x)
        ys.append(y)

    x_lo, x_hi = min(xs), max(xs)
    y_lo, y_hi = min(ys), max(ys)

    # Convert to tile indices
    h_min = int((x_lo - x_min_global) / TILE_SIZE)
    h_max = int((x_hi - x_min_global) / TILE_SIZE)
    v_min = int((y_max_global - y_hi) / TILE_SIZE)
    v_max = int((y_max_global - y_lo) / TILE_SIZE)

    # Clamp to valid range
    h_min = max(0, min(h_min, N_TILES_H - 1))
    h_max = max(0, min(h_max, N_TILES_H - 1))
    v_min = max(0, min(v_min, N_TILES_V - 1))
    v_max = max(0, min(v_max, N_TILES_V - 1))

    tiles = []
    for h in range(h_min, h_max + 1):
        for v in range(v_min, v_max + 1):
            tiles.append(f"h{h:02d}v{v:02d}")

    return sorted(tiles)
