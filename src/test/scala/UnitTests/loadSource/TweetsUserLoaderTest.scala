package UnitTests.loadSource

import loaders.TweetsUserLoader
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TweetsUserLoaderTest extends AnyFunSuite with BeforeAndAfterAll{


  private var spark: SparkSession = _
  private var tweetsUserLoader: TweetsUserLoader = _

  // Start spark session
  override def beforeAll(): Unit = {

    spark = SparkSession.builder()
      .appName("ETL-Project")
      .master("local[*]")
      .getOrCreate()

    tweetsUserLoader = new TweetsUserLoader(spark)

  }

  // Stop spark session
  override def afterAll(): Unit = {
    spark.stop()
  }

  test("should load data from twitter_dataset.csv and add category column") {
    val loadedData: Dataset[Row] = tweetsUserLoader.loadTwitter()
    // Check if the file is loaded
    assert(loadedData.count() > 0)
    // Check if the category data is added
    assert(loadedData.columns.contains("category"))
    // Check if the category column value is equals "forbes"
    assert(loadedData.select("category").head().getString(0) === "tweets")
  }

}
