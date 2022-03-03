import logging

import pyspark.sql.functions as f
from pyspark.sql import SparkSession


class ELTEngine:
    def __init__(self, spark: SparkSession, config: dict):
        self._spark = spark
        self._config = config
        self.first_df = None
        self.next_df = None

    def read_first_data(self):
        logging.info("Reading first data source")
        self.first_df = self._spark.read.option("header", True).format("csv").load(self._config["first_ds"])
        logging.info("Completed")

        self.first_df.show(3, False)

        return self

    def preprocess_first(self):
        logging.info("Preprocessing first data source")
        df = self.first_df.withColumn("full_filename", f.input_file_name())
        df = df.withColumn("filename", f.regexp_extract(df["full_filename"], "^.*[/\\\\]([^/\\\\]*?)$", 1))
        df = df.withColumn("districtName", f.regexp_extract(df["filename"], "^\\d+-\\d+-([\\w-]*?)-street.csv$", 1))
        # df.printSchema()

        df = df.select(
            f.col("Crime ID").alias("crimeId"),
            "districtName",
            f.col("Latitude").cast("float").alias("latitude"),
            f.col("Longitude").cast("float").alias("longitude"),
            f.col("Crime type").alias("crimeType"),
            f.col("Last outcome category").alias("lastOutcome")
        ).fillna("NA")

        # street_df.printSchema()
        # print("df count: " + str(df.count()))
        # df.show(3, False)

        self.first_df = df
        logging.info("Completed")

        return self

    def read_next_data(self):
        logging.info("Reading next data source")
        self.next_df = self._spark.read.option("header", True).format("csv").load(self._config["next_ds"])
        # outcomes_df = spark.read.option("header", True).format("csv").load("data/2019-01/2019-01-avon-and-somerset-outcomes.csv")
        # outcomes_df.printSchema()

        logging.info("Completed")
        return self

    def preprocess_next(self):
        logging.info("Preprocessing next data source")
        df = self.next_df.select(
            f.col("Crime ID").alias("crimeId"),
            f.col("Outcome type").alias("lastOutcome")
        ).dropDuplicates().dropna(subset=["crimeId", "lastOutcome"])

        self.next_df = df
        logging.info("Completed")

        return self

    def join_ds(self):
        logging.info("Joining data sources")
        df = self.first_df.join(self.next_df, self.first_df["crimeId"] == self.next_df["crimeId"], "left"). \
            select(self.first_df["crimeId"],
                   self.first_df["districtName"],
                   self.first_df["latitude"],
                   self.first_df["longitude"],
                   self.first_df["crimeType"],
                   f.when(self.next_df["crimeId"].isNotNull(), "lastOutcome").otherwise(self.first_df["lastOutcome"]).alias("lastOutcome")
                   )
        # repartition("districtName", "crimeType")

        # df.show(3, False)

        self.first_df = df
        logging.info("Completed")

        return self

    def write_to_file(self):
        logging.info("Writing output to file")
        self.first_df.write.mode("overwrite").format("parquet").save(self._config["output_path"])
        logging.info("Completed")

        return self
