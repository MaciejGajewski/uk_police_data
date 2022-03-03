import logging

import findspark

findspark.init()

from pyspark.sql import SparkSession

from src.elt.elt_engine import ELTEngine


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("info.log"),
        logging.StreamHandler()
    ]
)

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("UK_Data_Police_ETL") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

    config = {"first_ds": "landing_zone/*/*street.csv",
              "next_ds": "landing_zone/*/*outcomes.csv",
              "output_path": "./uk_police_data"
              }

    elt_engine = ELTEngine(spark, config)
    elt_engine.read_first_data().\
        read_next_data().\
        preprocess_first().\
        preprocess_next().\
        join_ds().\
        write_to_file()
