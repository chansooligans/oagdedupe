from dedupe.settings import Settings
from dedupe.block.schemes import BlockSchemes
from dedupe.db.database import Engine

from dataclasses import dataclass
import logging

class ForwardIndex(BlockSchemes):
    """
    Builds Entity Index (Forward Index) where keys are entities 
    and values are signatures
    """

    def query_blocks(self, table, columns):
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
        for table in ["sample","train"]:
            logging.info(f"Building forward indices: \
                {self.settings.other.db_schema}.blocks_{table}")
            self.engine.execute(self.query_blocks(
                table=table,
                columns=self.block_scheme_sql
            ))

    def build_forward_indices_full(self, columns):
        logging.info(f"Building forward indices: \
            {self.settings.other.db_schema}.blocks_df")
        self.engine.execute(self.query_blocks(
            table="df",
            columns=columns
        ))    
        
@dataclass
class Blocker(ForwardIndex, Engine):
    settings: Settings
    """
    DatabaseORM is used only for `self.engine`
    """
    pass