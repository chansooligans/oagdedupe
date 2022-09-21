from dedupe.settings import Settings
from dedupe.block.schemes import BlockSchemes
from dedupe.db.database import DatabaseORM

from dataclasses import dataclass
from functools import cached_property
import logging

class BlockSchemes:

    @property
    def block_schemes(self):
        return [
            ("first_nchars", [2,4,6]),
            ("last_nchars", [2,4,6]),
            ("find_ngrams",[2,4,6]),
            ("acronym", [None]),
            ("exactmatch", [None])
        ]

    @cached_property
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

    @cached_property
    def block_scheme_names(self):
        return [
            f"{scheme}_{n}_{attribute}"
            if n
            else f"{scheme}_{attribute}"
            for attribute in self.settings.other.attributes
            for scheme,nlist in self.block_schemes
            for n in nlist
        ]

    @cached_property
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


class ForwardIndex(BlockSchemes):
    """
    Builds Entity Index (Forward Index) where keys are entities and values are signatures
    """

    def query_blocks(self, table, columns):
        return f"""
            DROP TABLE IF EXISTS {self.schema}.blocks_{table};
            
            CREATE TABLE {self.schema}.blocks_{table} as (
                SELECT 
                    _index,
                    {", ".join(columns)}
                FROM {self.schema}.{table}
            );
        """

    def build_forward_indices(self):
        for table in ["sample","train"]:
            logging.info(f"Building forward indices: {self.schema}.blocks_{table}")
            self.engine.execute(self.query_blocks(
                table=table,
                columns=self.block_scheme_sql
            ))

    def build_forward_indices_full(self, columns):
        logging.info(f"Building forward indices: {self.schema}.blocks_df")
        self.engine.execute(self.query_blocks(
            table="df",
            columns=columns
        ))    
        
@dataclass
class Blocker(ForwardIndex, DatabaseORM):
    settings: Settings

    def __post_init__(self):
        self.schema = self.settings.other.db_schema