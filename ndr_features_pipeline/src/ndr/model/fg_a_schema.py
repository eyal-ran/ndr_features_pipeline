from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class FGAMeta:
    """Metadata describing a single FG-A feature."""
    feature_id: str
    name: str
    dtype: str
    description: str


def get_fga_features() -> List[FGAMeta]:
    """Return the list of FG-A features as defined in the external specification.

    For v1, this returns an empty list. A later revision should populate this
    from a JSON/CSV derived from the FG-A feature list document.
    """
    return []
