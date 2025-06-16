# ingestion_db.py (Fix)

import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# DO NOT configure logging here, just get a logger
logger = logging.getLogger(__name__)

# Create engine
engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    # This function will ingest the dataframe into database table
    df.to_sql(table_name, con=engine, if_exists='replace', index=False, chunksize=1000)
    
def load_raw_data():
    # This function will load the CSVs as dataframe and ingest into db
    start = time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df = pd.read_csv('data/' + file)
            logger.info(f'Ingesting {file} into database')
            ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time = (end - start) / 60
    logger.info('---------------------Ingestion Complete---------------------')
    logger.info(f'Total Time Taken: {total_time} minutes')

if __name__ == '__main__':
    # Only configure logging when run directly
    logging.basicConfig(
        filename="logs/ingestion_db.log",
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filemode="a"
    )
    load_raw_data()
