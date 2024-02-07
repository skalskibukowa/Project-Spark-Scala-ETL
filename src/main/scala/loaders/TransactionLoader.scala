package loaders

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class TransactionLoader(sparkSession: SparkSession) {

  def loadTransactions(): Dataset[Row] = sparkSession.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://host.docker.internal:5438/postgres")
    .option("dbtable", "\"Transaction\"")
    .option("user", "postgres")
    .option("password", "postgres")
    .load()

}
