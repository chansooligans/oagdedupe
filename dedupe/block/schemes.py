from oagdedupe.settings import Settings

class BlockSchemesHelper:
    """
    Attributes used to help build SQL queries, which are used to 
    build forward indices. 
    """

    @property
    def block_scheme_mapping(self):
        """
        helper to build column names in query
        """
        mapping = {}
        for attribute in self.settings.other.attributes:
            for scheme,nlist in self.block_schemes:
                for n in nlist:
                    if n:
                        mapping[f"{scheme}_{n}_{attribute}"]=f"{scheme}({attribute},{n})"
                    else:
                        mapping[f"{scheme}_{attribute}"]=f"{scheme}({attribute})"
        return mapping

    @property
    def block_scheme_sql(self):
        """
        helper to build column names in query
        """
        return [
            f"{scheme}({attribute},{n}) as {scheme}_{n}_{attribute}"
            if n
            else f"{scheme}({attribute}) as {scheme}_{attribute}"
            for attribute in self.settings.other.attributes
            for scheme,nlist in self.block_schemes
            for n in nlist
        ]

class BlockSchemes(BlockSchemesHelper):
    """
    Contains all block schemes.
    """
    settings: Settings

    @property
    def block_schemes(self):
        """
        List of tuples containing block scheme name and parameters.
        The block scheme name should correspond to a postgres function
        """
        return [
            ("first_nchars", [2,4,6]),
            ("last_nchars", [2,4,6]),
            ("find_ngrams",[2,4,6]),
            ("acronym", [None]),
            ("exactmatch", [None])
        ]

    @property
    def block_scheme_names(self):
        """
        Convenience property to list all block schemes
        """
        return [
            f"{scheme}_{n}_{attribute}"
            if n
            else f"{scheme}_{attribute}"
            for attribute in self.settings.other.attributes
            for scheme,nlist in self.block_schemes
            for n in nlist
        ]
