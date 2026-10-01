import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
import pytest
from pyspark.sql import SparkSession
 
from pyspark_job import clean_data
 
 
@pytest.fixture(scope="session")
def spark():
    session = (
    SparkSession.builder
    .master("local[1]")
    .appName("pyspark-ci-tests")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.python.worker.reuse", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
    )
    yield session
    session.stop()
 
 
def make_df(spark, rows):
    return spark.createDataFrame(rows, "name string, amount double")
 
 
def test_valid_records_are_kept(spark):
    df = make_df(spark, [("Alice", 100.0), ("Bob", 50.0)])
    result = clean_data(df)
    assert result.count() == 2
    assert {r["name"] for r in result.collect()} == {"Alice", "Bob"}
 
 
def test_amount_less_or_equal_zero_removed(spark):
    df = make_df(spark, [("Alice", 100.0), ("Bob", 0.0), ("Carol", -5.0)])
    result = clean_data(df)
    assert [r["name"] for r in result.collect()] == ["Alice"]
 
 
def test_null_names_removed(spark):
    df = make_df(spark, [("Alice", 100.0), (None, 80.0)])
    result = clean_data(df)
    assert result.count() == 1
    assert result.collect()[0]["name"] == "Alice"
 
 
def test_amount_with_tax_calculated_correctly(spark):
    df = make_df(spark, [("Alice", 100.0), ("Bob", 50.0)])
    result = {r["name"]: r["amount_with_tax"] for r in clean_data(df).collect()}
    assert result["Alice"] == pytest.approx(120.0)
    assert result["Bob"] == pytest.approx(60.0)
