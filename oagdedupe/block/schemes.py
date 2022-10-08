"""This module contains blocking schemes
"""
from typing import Dict, List, Optional, Tuple

from oagdedupe.settings import Settings


class BlockSchemes:
    """
    Attributes used to help build SQL queries, which are used to
    build forward indices.
    """

    settings: Settings

    @property
    def block_scheme_mapping(self) -> Dict[str, str]:
        """
        helper to build column names in query
        """
        mapping = {}
        for attribute in self.settings.attributes:
            for scheme, nlist in self.block_schemes:
                for n in nlist:
                    if n:
                        mapping[
                            f"{scheme}_{n}_{attribute}"
                        ] = f"{scheme}({attribute},{n})"
                    else:
                        mapping[
                            f"{scheme}_{attribute}"
                        ] = f"{scheme}({attribute})"
        return mapping

    @property
    def block_scheme_sql(self) -> List[str]:
        """
        helper to build column names in query
        """
        return [
            f"{scheme}({attribute},{n}) as {scheme}_{n}_{attribute}"
            if n
            else f"{scheme}({attribute}) as {scheme}_{attribute}"
            for attribute in self.settings.attributes
            for scheme, nlist in self.block_schemes
            for n in nlist
        ]

    @property
    def block_schemes(self) -> List[Tuple[str, List[Optional[int]]]]:
        """
        List of tuples containing block scheme name and parameters.
        The block scheme name should correspond to a postgres function
        """
        return [
            ("first_nchars", [2, 4, 6]),
            ("last_nchars", [2, 4, 6]),
            ("find_ngrams", [4, 6, 8]),
            ("acronym", [None]),
            ("exactmatch", [None]),
        ]

    @property
    def block_scheme_names(self) -> List[str]:
        """
        Convenience property to list all block schemes
        """
        return [
            f"{scheme}_{n}_{attribute}" if n else f"{scheme}_{attribute}"
            for attribute in self.settings.attributes
            for scheme, nlist in self.block_schemes
            for n in nlist
        ]
