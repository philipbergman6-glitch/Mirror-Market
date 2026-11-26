"""The verifier's own unit constants, derived from their published definitions.

Invariant 7, as amended by the owner on 2026-11-25 (#479): unit conversion has
two sites in the product; the verifier is outside the product and holds its
own constants, derived independently and compared by test. The comparison is
``tests/test_verify_units_tripwire.py``: it pins every constant here to the
figure its source prints, and to the product's ``pipeline/units.py`` within the
residue the product's rounded constants allow (2 × 10⁻⁶ relative).

Nothing here is copied from the product. Each value is built from exact
definitions, so the product's rounding shows up as a residue rather than being
inherited.
"""

from __future__ import annotations

# --- Exact definitions ------------------------------------------------------
# NIST Handbook 44 (2023), Appendix C, "General Tables of Units of
# Measurement": 1 avoirdupois pound = 0.453 592 37 kilogram exactly (the 1959
# international yard and pound agreement); 1 short ton = 2 000 pounds.
KG_PER_LB = 0.45359237
LB_PER_SHORT_TON = 2000.0
# The SI tonne: 1 metric ton = 1 000 kilograms (NIST HB44 Appendix C).
KG_PER_MT = 1000.0
# USDA ERS Agricultural Handbook 697, "Weights, Measures, and Conversion
# Factors for Agricultural Commodities and Their Products" (1992): a bushel of
# soybeans weighs 60 pounds; the handbook prints 1 metric ton = 36.7437 bu.
LB_PER_BUSHEL_SOYBEANS = 60.0
# CEPEA/ESALQ publish the Paraná and Paranaguá indicators in "reais por saca
# de 60 kg" (indicator page and Paranaguá methodology PDF): 1 sack = 60 kg.
KG_PER_SACK = 60.0

# --- Derived constants --------------------------------------------------------
LB_PER_MT = KG_PER_MT / KG_PER_LB                                      # 2,204.62262 lb/MT
BU_PER_MT_SOYBEANS = KG_PER_MT / (LB_PER_BUSHEL_SOYBEANS * KG_PER_LB)  # 36.74371 bu/MT
MT_PER_SHORT_TON = LB_PER_SHORT_TON * KG_PER_LB / KG_PER_MT            # 0.90718474 MT/st
SACKS_PER_MT = KG_PER_MT / KG_PER_SACK                                 # 16.66667 sc/MT

# The figure each constant is published as, and where. The tripwire test holds
# every derived constant to its printed figure at the figure's own precision.
PUBLISHED: dict[str, tuple[str, str]] = {
    "BU_PER_MT_SOYBEANS": ("36.74371", "USDA ERS AH-697 (60 lb/bu) with NIST HB44 App. C (lb = 0.45359237 kg)"),
    "LB_PER_MT": ("2204.62262", "NIST HB44 App. C (lb = 0.45359237 kg)"),
    "MT_PER_SHORT_TON": ("0.90718474", "NIST HB44 App. C (short ton = 2,000 lb)"),
    "SACKS_PER_MT": ("16.66667", "CEPEA/ESALQ indicator unit, saca de 60 kg"),
}
