package UnitTests.loaders

import loaders.ForbesLoader
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class ForbesLoaderTest extends AnyFunSuite {

  test("should load data from CSV and add category column") {
    // Create mock SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Test")
      .master("local[*]") // use all cores
      .getOrCreate()

    // Create instance of ForbesLoader
    val forbesLoader: ForbesLoader = new ForbesLoader(spark)

    // Load dataset
    val loadedData: Dataset[Row] = forbesLoader.loadForbes()

    // Assertions

    // Check if the file is loaded
    assert(loadedData.count() > 0)

    // Check if the category data is added
    assert(loadedData.columns.contains("category"))

    // Check if the category column value is equals "forbes"
    assert(loadedData.select("category").head().getString(0) === "forbes")

    spark.stop()
  }

}
