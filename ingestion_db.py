import os
import time
import logging
import pandas as pd
from sqlalchemy import create_engine

# Module-specific Logger 
logger = logging.getLogger("ingestion_db")
logger.setLevel(logging.DEBUG)

# Prevent duplicate handlers if module is imported multiple times
if not logger.handlers:
    handler = logging.FileHandler("logs/ingestion_db.log", mode='a', encoding='utf-8')
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# DB engine
engine = create_engine("sqlite:///inventory.db")

# Core helpers
def ingest_db(df: pd.DataFrame, table_name: str, engine_=engine) -> None:
    """Ingest *df* into *table_name* (replaces if table already exists)."""
    df.to_sql(table_name, con=engine_, if_exists="replace", index=False, chunksize=1_000)
    logger.info("Ingested %s rows into table '%s'", len(df), table_name)


def load_raw_data(data_dir: str = "data") -> None:
    """Read every CSV in *data_dir* and ingest it into its own table."""
    start = time.time()

    for file in os.listdir(data_dir):
        if file.lower().endswith(".csv"):
            path = os.path.join(data_dir, file)
            df = pd.read_csv(path)
            table_name = os.path.splitext(file)[0]

            logger.info("Ingesting %s into DB as table '%s'…", file, table_name)
            ingest_db(df, table_name)

    minutes = (time.time() - start) / 60
    logger.info("-- Ingestion complete (%.2f min) --", minutes)


if __name__ == "__main__":
    load_raw_data()