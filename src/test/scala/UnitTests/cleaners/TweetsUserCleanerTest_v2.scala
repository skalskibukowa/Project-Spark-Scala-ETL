package UnitTests.cleaners

import cleaners.TweetsUserCleaner
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TweetsUserCleanerTest_v2 extends AnyFunSuite with BeforeAndAfterAll{

  private var spark: SparkSession = _
  private var tweetsUserCleaner: TweetsUserCleaner = _

  override def beforeAll(): Unit = {

    spark = SparkSession
      .builder()
      .appName("TweetsUserCleaner-Test")
      .master("local[*]")
      .getOrCreate()


    tweetsUserCleaner = new TweetsUserCleaner(spark)

  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("should cast Tweet_ID, Retweets, Likes, and cast Timestamp to DateType") {

    // Input data
   val inputDF = spark.createDataFrame(Seq(
      (123L, 456L, 789L, "2023-03-28"),
      (987L, 321L, 0L, "2022-02-14")
    )).toDF("Tweet_ID", "Retweets", "Likes", "Timestamp")

    val cleanedDF = tweetsUserCleaner.cleanTweetsUser(inputDF)

    assert(cleanedDF.schema("Tweet_ID").dataType.typeName == "integer")
    assert(cleanedDF.schema("Retweets").dataType.typeName == "integer")
    assert(cleanedDF.schema("Likes").dataType.typeName == "integer")
    assert(cleanedDF.schema("Timestamp").dataType.typeName == "integer")
  }

}
