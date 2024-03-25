package cleaners

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class ForbesCleaner(sparkSession: SparkSession) {

  import sparkSession.implicits._

  def cleanForbes(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn("rank", col("rank").cast(DataTypes.IntegerType))
      .withColumn("rank", col("rank").cast("integer")).filter($"rank".isNotNull)
      .withColumn("age", col("age").cast(DataTypes.IntegerType))
      .withColumn("finalWorth", col("finalWorth").cast(DataTypes.IntegerType))
      .withColumn("year", col("year").cast(DataTypes.IntegerType))
      .withColumn("month", col("month").cast(DataTypes.IntegerType))
      .withColumn("birthDate", col("birthDate").cast(DataTypes.DateType))
      .withColumn("philanthropyScore", col("philanthropyScore").cast(DataTypes.FloatType))
      .na.fill("N/A")
  }
}