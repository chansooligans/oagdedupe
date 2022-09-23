from dedupe.settings import Settings
from dedupe.block.schemes import BlockSchemes
from dedupe.db.database import Engine

from dataclasses import dataclass
import logging
import pandas as pd

@dataclass
class Blocker(BlockSchemes, Engine):
    settings: Settings
    """
    Used to build forward indices. A forward index 
    is a table where rows are entities, columns are block schemes,
    and values contain signatures.

    Attributes
    ----------
    settings : Settings
    """

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

    def add_scheme(self, table, col, exists):
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

        logging.info(f"computing block scheme {col} on full data")

        self.engine.execute(f"""
            ALTER TABLE dedupe.blocks_{table}
            ADD COLUMN IF NOT EXISTS {col} {coltype}
        """)

        self.engine.execute(f"""
            UPDATE dedupe.blocks_{table} AS t1
            SET {col} = t2.{col}
            FROM (
                SELECT _index, {self.block_scheme_mapping[col]} as {col}
                FROM dedupe.{table}
            ) t2
            WHERE t1._index = t2._index;
        """)
        self.engine.dispose()
        return

    def build_forward_indices(self):
        """
        Executes SQL queries to build forward indices for train datasets
        """
    
        logging.info(f"Building forward indices: \
            {self.settings.other.db_schema}.blocks_train")
        self.engine.execute(self.query_blocks(
            table="train",
            columns=self.block_scheme_sql
        ))

    def init_forward_index_full(self):
        self.engine.execute(f"""
            DROP TABLE IF EXISTS {self.settings.other.db_schema}.blocks_df;
            
            CREATE TABLE {self.settings.other.db_schema}.blocks_df as (
                SELECT 
                    _index
                FROM {self.settings.other.db_schema}.df
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
                "SELECT * FROM dedupe.blocks_df LIMIT 1", 
                con=self.engine
            ).columns
        
            self.add_scheme(table="df", col=col, exists=exists)