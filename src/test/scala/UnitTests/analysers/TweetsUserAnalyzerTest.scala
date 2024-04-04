package UnitTests.analysers

import analysers.TweetsUserAnalyzer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TweetsUserAnalyzerTest extends AnyFunSuite with BeforeAndAfterAll{

  private var spark: SparkSession = _
  private var analyzer: TweetsUserAnalyzer = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("TweetsUserAnalyzer-Test")
      .master("local[*]")
      .getOrCreate()

    analyzer = new TweetsUserAnalyzer(spark)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  // Test method top10RetweetedPost
  test("top10RetweetedPost should return top 10 retweeted posts") {
      val data = spark.createDataFrame(Seq(
        (1, "JohnDoe", "This is a tweet", 100, 50, "2024-04-03"),
        (2, "JaneSmith", "Another tweet", 200, 25, "2024-04-02"),
        (3, "LiWang", "Interesting article", 250, 75, "2024-04-01"),
        (4, "AnnaKowalska", "Hello world!", 150, 10, "2024-04-03"),
        (5, "JohnDoe", "Second tweet", 75, 30, "2024-04-02"),
        (6, "JohnDoe", "This is a tweet", 130, 50, "2024-04-03"),
        (7, "JaneSmith", "Another tweet", 220, 25, "2024-04-02"),
        (8, "LiWang", "Interesting article", 350, 75, "2024-04-01"),
        (9, "AnnaKowalska", "Hello world!", 450, 10, "2024-04-03"),
        (10, "JohnDoe", "Second tweet", 75, 30, "2024-04-02"),
        (11, "JohnDoe", "This is a tweet", 800, 50, "2024-04-03"),
        (12, "JaneSmith", "Another tweet", 900, 25, "2024-04-02"),
        (13, "LiWang", "Interesting article", 1050, 75, "2024-04-01"),
        (14, "AnnaKowalska", "Hello world!", 1650, 10, "2024-04-03"),
        (15, "JohnDoe", "Second tweet", 75, 1430, "2024-04-02")
      )).toDF(TweetsUserAnalyzer.TWEET_ID, TweetsUserAnalyzer.USERNAME, TweetsUserAnalyzer.TEXT, TweetsUserAnalyzer.RETWEETS, TweetsUserAnalyzer.LIKES, TweetsUserAnalyzer.DATE)

    val filteredDF = analyzer.top10RetweetedPost(data)

    // Assert presence of top retweeted post
    assert(filteredDF.filter(col(TweetsUserAnalyzer.USERNAME) === "AnnaKowalska").select(TweetsUserAnalyzer.RETWEETS).head().getAs[Long](TweetsUserAnalyzer.RETWEETS) === 1650)

    // Assert expected number of rows (should be 10 or less if fewer rows are provided)
    assert(filteredDF.count() <= 10)
  }

  // Test method top10LikedPosts
  test("top10LikedPosts should return top 10 liked posts") {
    val data = spark.createDataFrame(Seq(
      (1, "JohnDoe", "This is a tweet", 100, 150, "2024-04-03"),
      (2, "JaneSmith", "Another tweet", 200, 125, "2024-04-02"),
      (3, "LiWang", "Interesting article", 250, 175, "2024-04-01"),
      (4, "AnnaKowalska", "Hello world!", 150, 110, "2024-04-03"),
      (5, "JohnDoe", "Second tweet", 75, 230, "2024-04-02"),
      (6, "JohnDoe", "This is a tweet", 130, 250, "2024-04-03"),
      (7, "JaneSmith", "Another tweet", 220, 225, "2024-04-02"),
      (8, "LiWang", "Interesting article", 350, 275, "2024-04-01"),
      (9, "AnnaKowalska", "Hello world!", 450, 210, "2024-04-03"),
      (10, "JohnDoe", "Second tweet", 75, 230, "2024-04-02"),
      (11, "JohnDoe", "This is a tweet", 800, 350, "2024-04-03"),
      (12, "JaneSmith", "Another tweet", 900, 325, "2024-04-02"),
      (13, "LiWang", "Interesting article", 1050, 375, "2024-04-01"),
      (14, "AnnaKowalska", "Hello world!", 1650, 310, "2024-04-03"),
      (15, "JohnDoe", "Second tweet", 75, 1430, "2024-04-02")
    )).toDF(TweetsUserAnalyzer.TWEET_ID, TweetsUserAnalyzer.USERNAME, TweetsUserAnalyzer.TEXT, TweetsUserAnalyzer.RETWEETS, TweetsUserAnalyzer.LIKES, TweetsUserAnalyzer.DATE)

    val filteredDF = analyzer.top10LikedPosts(data)

    // Assert presence of top liked post
    assert(filteredDF.filter(col(TweetsUserAnalyzer.USERNAME) === "LiWang").select(TweetsUserAnalyzer.LIKES).head().getAs[Long](TweetsUserAnalyzer.LIKES) === 375)

    // Assert expected number of rows (should be 10 or less if fewer rows are provided)
    assert(filteredDF.count() >= 10)

  }

  // Test method countUserPosts
  test("countUserPosts should count posts per user") {
    val data = spark.createDataFrame(Seq(
      (1, "JohnDoe", "This is a tweet", 100, 150, "2024-04-03"),
      (2, "JaneSmith", "Another tweet", 200, 125, "2024-04-02"),
      (3, "LiWang", "Interesting article", 250, 175, "2024-04-01"),
      (4, "AnnaKowalska", "Hello world!", 150, 110, "2024-04-03"),
      (5, "JohnDoe", "Second tweet", 75, 230, "2024-04-02"),
      (6, "JohnDoe", "This is a tweet", 130, 250, "2024-04-03"),
      (7, "JaneSmith", "Another tweet", 220, 225, "2024-04-02"),
      (8, "LiWang", "Interesting article", 350, 275, "2024-04-01"),
      (9, "AnnaKowalska", "Hello world!", 450, 210, "2024-04-03"),
      (10, "JohnDoe", "Second tweet", 75, 230, "2024-04-02"),
      (11, "JohnDoe", "This is a tweet", 800, 350, "2024-04-03"),
      (12, "JaneSmith", "Another tweet", 900, 325, "2024-04-02"),
      (13, "LiWang", "Interesting article", 1050, 375, "2024-04-01"),
      (14, "AnnaKowalska", "Hello world!", 1650, 310, "2024-04-03"),
      (15, "JohnDoe", "Second tweet", 75, 1430, "2024-04-02")
    )).toDF(TweetsUserAnalyzer.TWEET_ID, TweetsUserAnalyzer.USERNAME, TweetsUserAnalyzer.TEXT, TweetsUserAnalyzer.RETWEETS, TweetsUserAnalyzer.LIKES, TweetsUserAnalyzer.DATE)


    val filteredDF = analyzer.countUserPosts(data)

    // Assert presence and count of specific user
    assert(filteredDF.filter(col(TweetsUserAnalyzer.USERNAME) === "JohnDoe").select("count").head().getAs[Long]("count") === 6)
    assert(filteredDF.filter(col(TweetsUserAnalyzer.USERNAME) === "AnnaKowalska").select("count").head().getAs[Long]("count") === 3)
  }

  test("countWordsInTextColumn should count word occurrences") {
    val data = spark.createDataFrame(Seq(
      (1, "JohnDoe", "This is a tweet", 100, 150, "2024-04-03"),
      (2, "JaneSmith", "Another tweet", 200, 125, "2024-04-02"),
      (3, "LiWang", "Interesting article", 250, 175, "2024-04-01"),
      (4, "AnnaKowalska", "Hello world!", 150, 110, "2024-04-03"),
      (5, "JohnDoe", "Second tweet", 75, 230, "2024-04-02"),
      (6, "JohnDoe", "This is a tweet", 130, 250, "2024-04-03"),
      (7, "JaneSmith", "Another tweet", 220, 225, "2024-04-02"),
      (8, "LiWang", "Interesting article", 350, 275, "2024-04-01"),
      (9, "AnnaKowalska", "Hello world!", 450, 210, "2024-04-03"),
      (10, "JohnDoe", "Second tweet", 75, 230, "2024-04-02"),
      (11, "JohnDoe", "This is a tweet", 800, 350, "2024-04-03"),
      (12, "JaneSmith", "Another tweet", 900, 325, "2024-04-02"),
      (13, "LiWang", "Interesting article", 1050, 375, "2024-04-01"),
      (14, "AnnaKowalska", "Hello world!", 1650, 310, "2024-04-03"),
      (15, "JohnDoe", "Second tweet", 75, 1430, "2024-04-02"),
      (16, "AnnaKowalska", "Hello !", 1650, 310, "2024-04-03"),
    )).toDF(TweetsUserAnalyzer.TWEET_ID, TweetsUserAnalyzer.USERNAME, TweetsUserAnalyzer.TEXT, TweetsUserAnalyzer.RETWEETS, TweetsUserAnalyzer.LIKES, TweetsUserAnalyzer.DATE)

    val filteredDF = analyzer.countWordsInTextColumn(data)

    // Assert presence and count of word tweet
    assert(filteredDF.filter(col("Word") === "tweet").select(col("count")).head().getAs[Long]("count") === 9)

    // Assert presence and count of word Hello
    assert(filteredDF.filter(col("Word") === "Hello").select(col("count")).head().getAs[Long]("count") === 4)
  }

}
