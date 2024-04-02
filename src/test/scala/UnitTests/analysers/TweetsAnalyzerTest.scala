package UnitTests.analysers

import analysers.TweetsAnalyzer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TweetsAnalyzerTest extends AnyFunSuite with BeforeAndAfterAll{

  private var spark: SparkSession = _
  private var analyzer: TweetsAnalyzer = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("TweetsAnalyzer-Test")
      .master("local[*]")
      .getOrCreate()

    analyzer = new TweetsAnalyzer(spark)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }


  // Test calculateHashtags method

  test("calculateHashtags should correctly count hashtags") {
    val data = spark.createDataFrame(Seq(
      (1, "John Doe", Array("This is a tweet #spark #scala", "This is a tweet #spark #scala"), true, "Twitter Web Client", 1000),
      (2, "Jane Smith", Array("Another tweet #java"), false, "TweetDeck", 2000),
      (3, "Li Wang", Array("Interesting article #machinelearning", "Interesting article #machinelearning", "Interesting article #machinelearning"), true, "Twitter for Android", 3000),
      (4, "Anna Kowalska", Array("Hello world!"), false, "Tweet for iPhone", 4000),
      (5, "Null User", null, true, "Twitter for Android", 5000) // Null hashtags
    )).toDF("id", TweetsAnalyzer.USER_NAME, TweetsAnalyzer.HASHTAG_COLUMN, TweetsAnalyzer.IS_RETWEET_COLUMN, TweetsAnalyzer.SOURCE_COLUMN, TweetsAnalyzer.USER_FOLLOWERS)

    val hashtagsDF = analyzer.calculateHashtags(data)

    // Assert the count of specific hashtags
    assert(hashtagsDF.filter(col(TweetsAnalyzer.HASHTAG_COLUMN) === "This is a tweet #spark #scala").select("count").head().getAs[Long]("count") === 2)
    assert(hashtagsDF.filter(col(TweetsAnalyzer.HASHTAG_COLUMN) === "Interesting article #machinelearning").select("count").head().getAs[Long]("count") === 3)
  }

}
