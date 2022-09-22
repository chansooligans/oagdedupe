from dedupe.settings import Settings
from dedupe.block.schemes import BlockSchemes
from dedupe.db.database import Engine

from dataclasses import dataclass
import logging

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

    def build_forward_indices(self):
        """
        Executes SQL queries to build forward indices for sample 
        and train datasets
        """
        for table in ["sample","train"]:
            logging.info(f"Building forward indices: \
                {self.settings.other.db_schema}.blocks_{table}")
            self.engine.execute(self.query_blocks(
                table=table,
                columns=self.block_scheme_sql
            ))

    def build_forward_indices_full(self, columns):
        """
        Executes SQL queries to build forward indices on full data.

        Parameters
        ----------
        columns : List[str]
            block schemes to include in forward index
        """
        logging.info(f"Building forward indices: \
            {self.settings.other.db_schema}.blocks_df")
        self.engine.execute(self.query_blocks(
            table="df",
            columns=columns
        ))    