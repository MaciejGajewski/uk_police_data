import unittest

from pyspark.sql import SparkSession

from src.elt.elt_engine import ELTEngine


class TestELTEngine(unittest.TestCase):
    def setUp(self) -> None:
        spark = SparkSession \
            .builder \
            .appName("UK_Data_Police_ETL_Test") \
            .master("local[*]") \
            .enableHiveSupport() \
            .getOrCreate()

        config = {"first_ds": "../data/*/*street.csv",
                  "next_ds": "../data/*/*outcomes.csv",
                  }

        self._elt_engine = ELTEngine(spark, config)

        src_row = [
            ["40eec1b47cb466ee098f3f1b18d90974ace2c0b2bdbbfe2cc71ca19eba925911", "2019-01", "Avon and Somerset Constabulary", "Avon and Somerset Constabulary", "", "", "No location", "", "",
             "Further investigation is not in the public interest"],
            ["40eec1b47cb466ee098f3f1b18d90974ace2c0b2bdbbfe2cc71ca19eba925911", "2019-01", "Avon and Somerset Constabulary", "Avon and Somerset Constabulary", "", "", "No location", "", "",
             "Further investigation is not in the public interest"],
            [None, "2019-01", "Avon and Somerset Constabulary", "Avon and Somerset Constabulary", "", "", "No location", "", "",
             "Further investigation is not in the public interest"]
        ]

        src_schema = ["Crime ID", "Month", "Reported by", "Falls within", "Longitude", "Latitude", "Location", "LSOA code", "LSOA name", "Outcome type"]

        out_row = [
            ["40eec1b47cb466ee098f3f1b18d90974ace2c0b2bdbbfe2cc71ca19eba925911", "Further investigation is not in the public interest"]
        ]

        out_schema = ["crimeId", "lastOutcome"]

        src_df = spark.createDataFrame(src_row, src_schema)
        src_df.show()

        self._out_df = spark.createDataFrame(out_row, out_schema)
        self._out_df.show()

        self._elt_engine.next_df = src_df

    def test_preprocess_first(self):
        self._elt_engine.preprocess_next()
        res_df = self._elt_engine.next_df

        self.assertEqual(
            res_df.collect(), self._out_df.collect()
        )
