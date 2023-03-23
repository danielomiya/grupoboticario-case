import typing as t
from argparse import ArgumentParser
from collections import OrderedDict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def get_parser() -> ArgumentParser:
    """
    Returns an ArgumentParser object with basic config to run the job

    :return: the argument parser
    """
    p = ArgumentParser(
        prog="SparkTScript",
        description="Spark script used for data transformation/cleaning",
    )
    p.add_argument("--app-name")
    p.add_argument("-i", "--input", required=True)
    p.add_argument("-o", "--output", required=True)
    p.add_argument("--format", default="csv")
    return p


def get_spark(app_name: t.Optional[str]) -> SparkSession:
    """
    Creates and returns a SparkSession object with the given name,
    or gets an existing one if any

    :param app_name: the name of the Spark application
    :return: a Spark session
    """

    return SparkSession.builder.appName(app_name).getOrCreate()


def clean(cleaning: t.Callable[[DataFrame], DataFrame]) -> None:
    """
    This function reads a DataFrame, applies a cleaning transformation to it,
    and saves the result to a Google BigQuery table

    :param cleaning: a callable that accepts a DataFrame and returns a
                     transformed DataFrame
    """
    parser = get_parser()
    args = parser.parse_args()
    spark = get_spark(args.app_name)
    df = (
        spark.read.format(args.format)
        .option("header", "true")
        .option("inferSchema", "true")
        .load(args.input)
    )
    (
        df.transform(cleaning)
        .write.mode("overwrite")
        .format("bigquery")
        .option("table", f"staging.{args.output}")
        .save()
    )


if __name__ == "__main__":
    mapping = OrderedDict(
        [
            ("ID_MARCA", "brand_id"),
            ("MARCA", "brand_name"),
            ("ID_LINHA", "product_line_id"),
            ("LINHA", "product_line_name"),
            ("DATA_VENDA", "sale_date"),
            ("QTD_VENDA", "sale_amount"),
        ]
    )
    clean(
        lambda df: df.select(
            [
                F.col(old_name).alias(new_name)
                for old_name, new_name in mapping.items()
            ]
        )
    )
