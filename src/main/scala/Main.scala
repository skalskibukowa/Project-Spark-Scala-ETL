import analysers.{ForbesAnalyzer, TweetsAnalyzer, TweetsSearch}
import cleaners.{ForbesCleaner, TweetsCleaner}
import loaders.{ForbesLoader, TweetsLoader}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Main {

  // TODO:
  //  0. Review data. Schema
  //  1. Load data to Spark. Loader
  //  2. Clean data. Cleaner
  //  3. Analyze data. Analyzer

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("twitterProject")
      .master("local[*]") // use all cores
      .getOrCreate()


    // Validate schema
/*
    val grammysDF: Dataset[Row] = spark.read
      .option("header", "true")
      .csv("GRAMMYs_tweets.csv")

    grammysDF.show(false)
    grammysDF.printSchema()


    val financialDF: Dataset[Row] = spark.read
      .option("header", "true")
      .csv("financial.csv")

    financialDF.show(false)
    financialDF.printSchema()

    val covidDF: Dataset[Row] = spark.read
      .option("header", "true")
      .csv("covid19_tweets.csv")

    covidDF.show(false)
    covidDF.printSchema()

    val forbesDF: Dataset[Row] = spark.read
      .option("header", "true")
      .option("sep", ",")
      .csv("forbes_2022_billionaires.csv")
      .drop("bio", "about")


    filteredForbesDF.show(false)
    filteredForbesDF.printSchema()

    val twitterDF: Dataset[Row] = spark.read
      .option("header", "true")
      .option("sep", ",")
      .csv("twitter_dataset.csv")

    twitterDF.show(false)
    twitterDF.printSchema()

    println(covidDF.count())
    println(financialDF.count())
    println(grammysDF.count())
    println(twitterDF.count())
 */


   // Tweets create objects
    val tweetsLoader: TweetsLoader = new TweetsLoader(spark)
    val tweetsCleaner: TweetsCleaner = new TweetsCleaner(spark)
    val tweetsSearch: TweetsSearch = new TweetsSearch(spark)
    val tweetsAnalyzer: TweetsAnalyzer = new TweetsAnalyzer(spark)

    // Forbes create object
    val forbesLoader: ForbesLoader = new ForbesLoader(spark)
    val forbesCleaner: ForbesCleaner = new ForbesCleaner(spark)
    val forbesAnalyzer: ForbesAnalyzer = new ForbesAnalyzer(spark)

    import tweetsSearch._

    // Import and clean -- Tweets
    val tweetsDF: Dataset[Row] = tweetsLoader.loadAllTweets().cache()
    val tweetsCleanedDF: Dataset[Row] = tweetsCleaner.cleanAllTweets(tweetsDF)

    // Import and clean -- Forbes

    val forbesDF: Dataset[Row] = forbesLoader.loadForbes().cache()
    val forbesCleanedDF: Dataset[Row] = forbesCleaner.cleanForbes(forbesDF)


    // Tweets Analyze
    // Search specific

    val trumpTweetsDF: Dataset[Row] = tweetsCleanedDF.transform(searchByKeyWord("Trump"))
      .transform(onlyInLocation("United States"))

    // Analytics
    val sourceCount: Dataset[Row] = tweetsAnalyzer.calculateSourceCount(trumpTweetsDF)
    val hashtagCount: Dataset[Row] = tweetsAnalyzer.calculateHashtags(trumpTweetsDF)


    sourceCount.show()
    hashtagCount.show()
    tweetsCleanedDF.show()

    // Forbes Analyze

    val underFiftyForbes: Dataset[Row] = forbesAnalyzer.filterUnderFiftyAgeForbes(forbesCleanedDF)
    val countCountriesForbes: Dataset[Row] = forbesAnalyzer.countCountriesForbes(forbesCleanedDF)

    underFiftyForbes.show()
   // TOP 4 Countries with Forbes people
    countCountriesForbes.show(4)

  }
}