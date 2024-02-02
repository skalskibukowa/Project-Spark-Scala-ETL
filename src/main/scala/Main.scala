import analysers.{ForbesAnalyzer, TweetsAnalyzer, TweetsSearch, TweetsUserAnalyzer}
import cleaners.{ForbesCleaner, TweetsCleaner, TweetsUserCleaner}
import loaders.{ForbesLoader, TweetsLoader, TweetsUserLoader}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.time.LocalDate


object Main {

  // TODO:
  //  0. Review data. Schema
  //  1. Load data to Spark. Loader
  //  2. Clean data. Cleaner
  //  3. Analyze data. Analyzer
  //  4. Save data to parquet/csv format

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

   // ********** Tweets ETL

   // Tweets create objects
    val tweetsLoader: TweetsLoader = new TweetsLoader(spark)
    val tweetsCleaner: TweetsCleaner = new TweetsCleaner(spark)
    val tweetsSearch: TweetsSearch = new TweetsSearch(spark)
    val tweetsAnalyzer: TweetsAnalyzer = new TweetsAnalyzer(spark)

   import tweetsSearch._

   // Import and clean -- Tweets
   val tweetsDF: Dataset[Row] = tweetsLoader.loadAllTweets().cache()
   val tweetsCleanedDF: Dataset[Row] = tweetsCleaner.cleanAllTweets(tweetsDF)

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

   // ********** Tweets ETL END

   // ********** Forbes ETL

   // Forbes create object
    val forbesLoader: ForbesLoader = new ForbesLoader(spark)
    val forbesCleaner: ForbesCleaner = new ForbesCleaner(spark)
    val forbesAnalyzer: ForbesAnalyzer = new ForbesAnalyzer(spark)

   // Import and clean -- Forbes

   val forbesDF: Dataset[Row] = forbesLoader.loadForbes().cache()
   val forbesCleanedDF: Dataset[Row] = forbesCleaner.cleanForbes(forbesDF)

   // Forbes Analyze

   val under50Forbes: Dataset[Row] = forbesAnalyzer.filterUnderFiftyAgeForbes(forbesCleanedDF)
   val countCountriesForbes: Dataset[Row] = forbesAnalyzer.countCountriesForbes(forbesCleanedDF)
   val top10SelfMadeForbes: Dataset[Row] = forbesAnalyzer.top10SelfMade(forbesCleanedDF)

   under50Forbes.show()
   // TOP 4 Countries with Forbes people
   countCountriesForbes.show(4)

   // TOP 10 Self Made Forbes
   top10SelfMadeForbes.show()

   // ********** Forbes ETL END

   // *********** TweetsUser ETL

   // TweetsUser create objects
   val tweetsUserLoader: TweetsUserLoader = new TweetsUserLoader(spark)
   val tweetsUserCleaner: TweetsUserCleaner = new TweetsUserCleaner(spark)
   val tweetsUserAnalyzer: TweetsUserAnalyzer = new TweetsUserAnalyzer(spark)

   // Import and clean -- TweetsUser

   val tweetsUserDF: Dataset[Row] = tweetsUserLoader.loadTwitter().cache()
   val tweetsUserCleanedDF: Dataset[Row] = tweetsUserCleaner.cleanTweetsUser(tweetsUserDF)

   // TweetsUser Analyze

   val countTextTweets: Dataset[Row] = tweetsUserAnalyzer.countWordsInTextColumn(tweetsUserCleanedDF)
   val countUserPosts: Dataset[Row] = tweetsUserAnalyzer.countUserPosts(tweetsUserCleanedDF)
   val top10LikedPosts: Dataset[Row] = tweetsUserAnalyzer.top10LikedPosts(tweetsUserCleanedDF)

   countTextTweets.show()
   countUserPosts.show()
   top10LikedPosts.show()

   val currentDate = LocalDate.now().toString

   // save to parquet format file
   /*
   top10LikedPosts.write
     .parquet("Sink/top10LikedPosts.parquet")
    */

   /*
   // save to csv format file
   top10LikedPosts.write
     .csv(s"Sink/top10LikedPosts_$currentDate.csv")
    */
   // *********** TweetsUser ETL END

  }

}