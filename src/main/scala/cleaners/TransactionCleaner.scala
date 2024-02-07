package cleaners

import org.apache.spark.sql.functions.{col, trim}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class TransactionCleaner(sparkSession: SparkSession) {

  def cleanAllTransactions(df: Dataset[Row]): Dataset[Row] ={
    df.withColumn("ID", trim(col("ID")))
      .withColumn("first_name", trim(col("first_name")))
      .withColumn("last_name", trim(col("last_name")))
      .withColumn("email", trim(col("email")))
      .withColumn("gender", trim(col("gender")))
      .withColumn("Currency", trim(col("Currency")))
      .withColumn("Product", trim(col("Product")))
      .withColumn("Country", trim(col("Country")))
      .withColumn("Postal_Code", trim(col("Postal_Code")))
      .withColumn("Street_Address", trim(col("Street_Address")))
  }

}
