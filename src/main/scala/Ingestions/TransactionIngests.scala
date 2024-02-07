package Ingestions

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class TransactionIngests(sparkSession: SparkSession) {

  def ingestTransactionUSA(df: Dataset[Row]): Unit =
    df.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://host.docker.internal:5438/postgres")
    .option("dbtable", "\"Transaction_USA\"")
    .option("user", "postgres")
    .option("password", "postgres")
    .save()

  def ingestTransactionFrance(df: Dataset[Row]): Unit =
    df.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://host.docker.internal:5438/postgres")
    .option("dbtable", "\"Transaction_France\"")
    .option("user", "postgres")
    .option("password", "postgres")
    .save()

  def ingestTransactionChina(df: Dataset[Row]): Unit =
    df.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://host.docker.internal:5438/postgres")
    .option("dbtable", "\"Transaction_China\"")
    .option("user", "postgres")
    .option("password", "postgres")
    .save()

  def ingestTransactionPoland(df: Dataset[Row]): Unit =
    df.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://host.docker.internal:5438/postgres")
    .option("dbtable", "\"Transaction_Poland\"")
    .option("user", "postgres")
    .option("password", "postgres")
    .save()

}