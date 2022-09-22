from functools import cached_property

class BlockSchemesHelper:

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
    def block_scheme_names(self):
        return [
            f"{scheme}_{n}_{attribute}"
            if n
            else f"{scheme}_{attribute}"
            for attribute in self.settings.other.attributes
            for scheme,nlist in self.block_schemes
            for n in nlist
        ]

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

    @property
    def block_schemes(self):
        return [
            ("first_nchars", [2,4,6]),
            ("last_nchars", [2,4,6]),
            ("find_ngrams",[2,4,6]),
            ("acronym", [None]),
            ("exactmatch", [None])
        ]
