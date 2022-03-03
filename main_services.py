import logging

from pyspark.sql import SparkSession

from src.kpi.kpi_engine import KPIEngine


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("UK_Data_Police_Services") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

    config = {"output_path": "./uk_police_data"}

    kpi_engine = KPIEngine(spark, config)

    print(kpi_engine.get_unique_crimes())
    print(kpi_engine.get_location_hotspots_count())
    print(kpi_engine.get_crimes_types_by_location_count())
    print(kpi_engine.get_crimes_by_crime_types())
