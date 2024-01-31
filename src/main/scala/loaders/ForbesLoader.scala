package loaders

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.lit


object ForbesLoader {
  val FORBES_LABEL: String = "forbes"
}

class ForbesLoader(sparkSession: SparkSession) {

  def loadForbes(): Dataset[Row] = sparkSession.read
    .option("header", "true")
    .csv("Datasource/forbes_2022_billionaires.csv")
    .withColumn("category", lit(ForbesLoader.FORBES_LABEL))
    .drop("bio", "about")
}
