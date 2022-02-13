from typing import List, Union, Any, Optional, Dict
from dataclasses import dataclass
from oagdedupe.base import BaseBlocker

@dataclass
class BlockerMixin(BaseBlocker):
    """ Abstract base class for all blockers to inherit
    """

    def block_map:
        "use raw data to get block maps"
        return block_map

    


