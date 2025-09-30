"""Utilities for global markets data and analysis.

This package exposes modules:
- world_indices: data collection utilities (prices/valuations)
- world_returns: flexible-period return calculations
- fx_rates: FX cache helpers
"""

# Re-export common modules for convenience
from . import world_indices  # noqa: F401
from . import world_returns  # noqa: F401
from . import fx_rates       # noqa: F401

