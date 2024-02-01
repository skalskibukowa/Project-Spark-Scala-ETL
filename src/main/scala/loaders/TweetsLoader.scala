package loaders

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object TweetsLoader {
  val COVID_LABEL: String = "covid"
  val GRAMMYS_LABEL: String = "grammys"
  val FINANCE_LABEL: String = "finance"
  val FORBES_LABEL: String = "forbes"
  val TWITTER_LABEL: String = "tweets"
}

class TweetsLoader(sparkSession: SparkSession) {

  def loadAllTweets(): Dataset[Row]={
    val covidDF: Dataset[Row] = loadCovid()
    val financialDF: Dataset[Row] = loadFinance()
    val grammysDF: Dataset[Row] = loadGrammys()
    val twitterDF: Dataset[Row] = loadTwitter()

    covidDF.unionByName(financialDF, true)
      .unionByName(grammysDF, true)
      .unionByName(twitterDF, true)
  }

  def loadCovid(): Dataset[Row] = sparkSession.read
    .option("header", "true")
    .csv("Datasource/covid19_tweets.csv")
    .withColumn("category", lit(TweetsLoader.COVID_LABEL))
    .na.drop()

  def loadFinance(): Dataset[Row] = sparkSession.read
    .option("header", "true")
    .csv("Datasource/financial.csv")
    .withColumn("category", lit(TweetsLoader.FINANCE_LABEL))

  def loadGrammys(): Dataset[Row] = sparkSession.read
    .option("header", "true")
    .csv("Datasource/GRAMMYs_tweets.csv")
    .withColumn("category", lit(TweetsLoader.GRAMMYS_LABEL))
    .na.drop()


}
