/*
package UnitTests.cleaners

import cleaners.TweetsUserCleaner
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TweetsUserCleanerTest extends AnyFunSuite with BeforeAndAfterAll{

  private var spark: SparkSession = _
  private var tweetsUserCleaner: TweetsUserCleaner = _
  private var inputDF: DataFrame = _

  override def beforeAll(): Unit = {

    spark = SparkSession
      .builder()
      .appName("TweetsUserCleaner-Test")
      .master("local[*]")
      .getOrCreate()


    tweetsUserCleaner = new TweetsUserCleaner(spark)

    // Input data
    inputDF = spark.createDataFrame(Seq(
      (123L, 456L, 789L, lit("2023-03-28")),
      (987L, 321L, 0L, lit("2022-02-14"))
    )).toDF("Tweet_ID", "Retweets", "Likes", "Timestamp")

  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("should cast Tweet_ID, Retweets, Likes, and cast Timestamp to DateType") {

    val cleanedDF = tweetsUserCleaner.cleanTweetsUser(inputDF)

    assert(cleanedDF.schema("Tweet_ID").dataType.typeName == "integer")
    assert(cleanedDF.schema("Retweets").dataType.typeName == "integer")
    assert(cleanedDF.schema("Likes").dataType.typeName == "integer")
    assert(cleanedDF.schema("Timestamp").dataType.typeName == "integer")
  }
}
*/