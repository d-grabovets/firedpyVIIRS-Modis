"""
VNP64A1 and MCD64A1 Burned Area QA Bit-Mask Decoder

Decodes QA values from VIIRS and MODIS burned area products.
Implements bit-level masking for land/water, validity, mapping period,
detection method, and special condition codes.
"""

import numpy as np

# Map special condition codes (bits 5-7) to human-readable labels
SPECIAL_CONDITION_LABELS = {
    0: "normal",
    1: "too_few_cloud_free",
    2: "too_few_training",
    3: "no_nearby_training",
    4: "different_month_training",
    5: "persistent_hotspot",
}


def decode_qa_bits(qa_value: int) -> dict:
    """
    Decode a single VNP64A1 QA value into individual bit fields.

    Parameters
    ----------
    qa_value : int
        QA value (0-255) from a single pixel

    Returns
    -------
    dict
        Dictionary with keys:
        - 'is_land': bool (bit 0)
        - 'is_valid': bool (bit 1)
        - 'is_normal_period': bool (bit 2 inverted: True if normal)
        - 'is_direct_detection': bool (bit 3 inverted: True if direct)
        - 'special_condition_code': int (bits 5-7, 0-5)
        - 'special_condition_label': str
    """
    is_land = bool((qa_value >> 0) & 1)
    is_valid = bool((qa_value >> 1) & 1)
    is_normal_period = not bool((qa_value >> 2) & 1)  # inverted: 0=normal, 1=shortened
    is_direct_detection = not bool((qa_value >> 3) & 1)  # inverted: 0=direct, 1=relabeled
    special_code = (qa_value >> 5) & 0b111  # bits 5-7

    return {
        'is_land': is_land,
        'is_valid': is_valid,
        'is_normal_period': is_normal_period,
        'is_direct_detection': is_direct_detection,
        'special_condition_code': special_code,
        'special_condition_label': SPECIAL_CONDITION_LABELS.get(special_code, 'unknown'),
    }


def parse_burn_qa(qa_array: np.ndarray, mode: str = "standard") -> np.ndarray:
    """
    Decode VNP64A1 / MCD64A1 QA bit-mask and return validity mask.

    Applies bit-level masking to QA values and returns a boolean array
    indicating which pixels are valid for analysis based on land/water status,
    data availability, mapping period, and special condition codes.

    Parameters
    ----------
    qa_array : numpy.ndarray
        Array of uint8 QA values from burned area product
    mode : str, default "standard"
        Filtering mode controlling strictness of validity criteria:

        - "strict": Requires land (bit0=1) AND valid (bit1=1) AND
          normal period (bit2=0) AND no special conditions (code==0)
        - "standard": Requires land (bit0=1) AND valid (bit1=1) AND
          special condition code < 3 (allows 'normal', 'too_few_cloud_free')
        - "permissive": Requires only land (bit0=1) AND valid (bit1=1)

    Returns
    -------
    numpy.ndarray (dtype=bool)
        Boolean array same shape as qa_array. True indicates pixel meets
        criteria for the selected mode and should be kept.

    Raises
    ------
    ValueError
        If mode not in ["strict", "standard", "permissive"]
    """
    if mode not in ["strict", "standard", "permissive"]:
        raise ValueError(f"mode must be 'strict', 'standard', or 'permissive', got '{mode}'")

    # Ensure uint8 dtype
    qa_array = np.asarray(qa_array, dtype=np.uint8)

    # Extract individual bits using bitwise operations (vectorized)
    is_land = (qa_array >> 0) & 1  # bit 0
    is_valid = (qa_array >> 1) & 1  # bit 1
    is_normal_period = ~((qa_array >> 2) & 1) & 1  # bit 2, inverted
    special_code = (qa_array >> 5) & 0b111  # bits 5-7

    # Base requirement: must be land and valid
    base_mask = (is_land == 1) & (is_valid == 1)

    if mode == "strict":
        # Also require normal mapping period and no special conditions
        mask = base_mask & (is_normal_period == 1) & (special_code == 0)
    elif mode == "standard":
        # Allow some special conditions (code < 3)
        mask = base_mask & (special_code < 3)
    else:  # "permissive"
        # Only require land and valid
        mask = base_mask

    return mask.astype(bool)
