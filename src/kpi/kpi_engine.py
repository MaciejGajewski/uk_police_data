import logging
from typing import List

import pyspark.sql.functions as f
from pyspark.sql import SparkSession


class KPIEngine:
    def __init__(self, spark: SparkSession, config: dict):
        self._spark = spark
        self._config = config

        logging.info("[KPIEngine] Initializing with data from " + self._config["output_path"])
        self._data_df = self._spark.read.format("parquet").load(self._config["output_path"]).cache()
        logging.info("Completed")

        self._data_df.show(3, False)

    def get_unique_crimes(self) -> List[str]:
        logging.info("Generating unique crimes")
        res_df = self._data_df.select('crimeType').distinct()

        res_df.show(20, False)
        logging.info("Completed")

        return res_df.toJSON().collect()

    def get_location_hotspots_count(self) -> List[str]:
        logging.info("Generating location hotspots")
        res_df = self._data_df.groupby('districtName')\
            .count().alias("count")\
            .sort(f.desc("count"))

        res_df.show(20, False)
        logging.info("Completed")

        return res_df.toJSON().collect()

    def get_crimes_types_by_location_count(self) -> List[str]:
        logging.info("Generating crimes by location")
        res_df = self._data_df.groupby('districtName', "crimeType")\
            .count().alias("count")\
            .sort(f.desc("districtName"), f.desc("count"))

        res_df.show(20, False)
        logging.info("Completed")

        return res_df.toJSON().collect()

    def get_crimes_by_crime_types(self) -> List[str]:
        logging.info("Generating crimes by crime type")
        res_df = self._data_df.groupby('crimeType')\
            .count().alias("count")\
            .sort(f.desc("count"))

        res_df.show(20, False)
        logging.info("Completed")

        return res_df.toJSON().collect()
