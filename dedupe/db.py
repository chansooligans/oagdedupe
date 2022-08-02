from dataclasses import dataclass
from sqlalchemy import create_engine
import pandas as pd

class CreateDB:

    def __init__(self, cache_fp:str):
        self.engine = create_engine(f"sqlite:///{cache_fp}", echo=False)

    def create_tables(self, X, idxmat, attributes):
        
        (
            self.df[attributes]
            .reset_index()
            .rename({"index":"idx"},axis=1)
            .to_sql("df", con=self.engine, if_exists="replace", index=False)
        )

        (
            pd.DataFrame(X)
            .to_sql("distances", con=self.engine, if_exists="replace", index=False)
        )

        (
            pd.DataFrame(idxmat, columns=["idxl","idxr"])
            .reset_index()
            .rename({"index":"idx"},axis=1)
            .to_sql("idxmat", con=self.engine, if_exists="replace", index=False)
        )

        self.engine.execute("DROP TABLE IF EXISTS query_index")
        self.engine.execute("""
            CREATE TABLE query_index (
                idx VARCHAR(255) NOT NULL
            )
        """)

        self.engine.execute("DROP TABLE IF EXISTS labels")
        self.engine.execute(f"""
            CREATE TABLE labels (
                idx int NOT NULL,
                label int,
                {
                    ", ".join([
                        _ + " VARCHAR(255)"
                        for _ in [x+"_l" for x in attributes] + [x+"_r" for x in attributes]
                    ])
                } 
            )
        """)