package UnitTests.cleaners


import cleaners.TweetsCleaner
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TweetsCleanerTest extends AnyFunSuite with BeforeAndAfterAll {


  private var spark: SparkSession = _
  private var tweetsCleaner: TweetsCleaner = _
  private var inputDF: DataFrame = _

  override def beforeAll(): Unit = {

    spark = SparkSession
      .builder()
      .appName("TweetsCleanerTest")
      .master("local[*]")
      .getOrCreate()


    tweetsCleaner = new TweetsCleaner(spark)

    inputDF = spark.createDataFrame(Seq(
      ("['#tag1, #tag2']", "2023-03-25", "2021-11-01", "4567", "123", "890"),
      ("tag3, #tag4]", "2022-01-12", "2020-05-17", "9876", "321", "054")
    )).toDF("hashtags", "date", "user_created", "user_favourites", "user_friends", "user_followers")

  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("should clean hashtags, cast dates, and cast numeric columns") {

    val cleanedDF = tweetsCleaner.cleanAllTweets(inputDF)

    assert(cleanedDF.select("hashtags").head().getAs[String]("hashtags") === Array("#tag1", " #tag2"))
    assert(cleanedDF.schema("date").dataType.typeName === "date")
    assert(cleanedDF.schema("user_created").dataType.typeName === "date")
    assert(cleanedDF.schema("user_favourites").dataType.typeName === "long")
    assert(cleanedDF.schema("user_friends").dataType.typeName === "long")
    assert(cleanedDF.schema("user_followers").dataType.typeName === "long")
  }

}