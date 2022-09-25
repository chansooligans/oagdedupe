from oagdedupe.settings import Settings
from oagdedupe.block.schemes import BlockSchemes
from oagdedupe.db.database import Engine
from oagdedupe import utils as du

from dataclasses import dataclass
import logging
import pandas as pd

@dataclass
class Blocker(BlockSchemes, Engine):
    """
    Used to build forward indices. A forward index 
    is a table where rows are entities, columns are block schemes,
    and values contain signatures.

    Attributes
    ----------
    settings : Settings
    """
    settings: Settings

    def query_blocks(self, table, columns):
        """
        Builds SQL query used to build forward indices. 

        Parameters
        ----------
        table : str
        columns : List[str]

        Returns
        ----------
        str
        """
        
        return f"""
            DROP TABLE IF EXISTS {self.settings.other.db_schema}.blocks_{table};
            
            CREATE TABLE {self.settings.other.db_schema}.blocks_{table} as (
                SELECT 
                    _index,
                    {", ".join(columns)}
                FROM {self.settings.other.db_schema}.{table}
            );
        """
        
    @du.recordlinkage_repeat
    def add_scheme(self, table, col, exists, rl=""):
        """
        check if column is in exists
        if not, add to blocks_{tablle}
        """

        if col in exists:
            return

        if "ngrams" in col:
            coltype = "text[]"
        else:
            coltype = "text"

        self.engine.execute(f"""
            ALTER TABLE {self.settings.other.db_schema}.blocks_{table}{rl}
            ADD COLUMN IF NOT EXISTS {col} {coltype}
        """)

        self.engine.execute(f"""
            UPDATE {self.settings.other.db_schema}.blocks_{table}{rl} AS t1
            SET {col} = t2.{col}
            FROM (
                SELECT _index, {self.block_scheme_mapping[col]} as {col}
                FROM {self.settings.other.db_schema}.{table}{rl}
            ) t2
            WHERE t1._index = t2._index;
        """)
        self.engine.dispose()
        return

    @du.recordlinkage_repeat
    def build_forward_indices(self, rl=""):
        """
        Executes SQL queries to build forward indices for train datasets
        """
    
        logging.info(f"Building forward indices")
        self.engine.execute(self.query_blocks(
            table=f"train{rl}",
            columns=self.block_scheme_sql
        ))

    @du.recordlinkage_repeat
    def init_forward_index_full(self, rl=""):
    
        self.engine.execute(f"""
            DROP TABLE IF EXISTS {self.settings.other.db_schema}.blocks_df{rl};
            
            CREATE TABLE {self.settings.other.db_schema}.blocks_df{rl} as (
                SELECT 
                    _index
                FROM {self.settings.other.db_schema}.df{rl}
            );
        """)

    def build_forward_indices_full(self, columns):
        """
        Executes SQL queries to build forward indices on full data.

        Parameters
        ----------
        columns : List[str]
            block schemes to include in forward index
        """
        for col in columns:
            exists = pd.read_sql(
                f"SELECT * FROM {self.settings.other.db_schema}.blocks_df LIMIT 1", 
                con=self.engine
            ).columns
        
            self.add_scheme(table="df", col=col, exists=exists)
