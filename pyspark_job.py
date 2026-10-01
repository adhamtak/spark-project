from pyspark.sql import DataFrame
from pyspark.sql import functions as F
 
 
def clean_data(df: DataFrame) -> DataFrame:
    """Drop invalid rows and add a tax-inclusive amount column."""
    return (
        df.filter(F.col("amount") > 0)          # removes amount <= 0 (and NULL amounts)
          .filter(F.col("name").isNotNull())    # removes NULL names
          .withColumn("amount_with_tax", F.col("amount") * 1.20)
    )
 

# trigger CI
