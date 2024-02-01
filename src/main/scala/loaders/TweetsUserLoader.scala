package loaders

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.lit

object TweetsUserLoader {
  val TWITTER_LABEL: String = "tweets"
}

class TweetsUserLoader(sparkSession: SparkSession) {
  def loadTwitter(): Dataset[Row] = sparkSession.read
    .option("header", "true")
    .csv("Datasource/twitter_dataset.csv")
    .withColumn("category", lit(TweetsUserLoader.TWITTER_LABEL))
}
