package UnitTests.loaders

import loaders.TweetsLoader
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TweetsLoaderTest extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tweetsLoader: TweetsLoader = _


  // Start spark session
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("ETL-Project")
      .master("local[*]")
      .getOrCreate()

    tweetsLoader = new TweetsLoader(spark)
  }

// Stop spark session
  override def afterAll(): Unit = {
    spark.stop()
  }

  test("should load data from covid19_tweets.csv and add category column") {
    val loadedData: Dataset[Row] = tweetsLoader.loadCovid()
    assert(loadedData.columns.contains("category"))
    assert(loadedData.select("category").head().getString(0) === "covid")
  }

  test("should load data from financial.csv and add category column") {
    val loadedData: Dataset[Row] = tweetsLoader.loadFinance()
    assert(loadedData.columns.contains("category"))
    assert(loadedData.select("category").head().getString(0) === "finance")
  }

  test("should load data from GRAMMYs_tweets.csv and add category column") {
    val loadedData: Dataset[Row] = tweetsLoader.loadGrammys()
    assert(loadedData.columns.contains("category"))
    assert(loadedData.select("category").head().getString(0) === "grammys")
  }



}
