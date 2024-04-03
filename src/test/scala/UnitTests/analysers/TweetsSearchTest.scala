package UnitTests.analysers

import analysers.TweetsSearch
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TweetsSearchTest extends AnyFunSuite with BeforeAndAfterAll{

  private var spark: SparkSession = _
  private var analyzer: TweetsSearch = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("TweetsSearch-Test")
      .master("local[*]")
      .getOrCreate()


    analyzer = new TweetsSearch(spark)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  // Test method searchByKeyWord
  test("searchByKeyWord should filter tweets containing keyword") {

    val data = spark.createDataFrame(Seq(
      (1, "John Doe", "This is a tweet about Spark", "US"),
      (2, "Jane Smith", "Another tweet on Scala", "UK"),
      (3, "Li Wang", "Interesting article on Machine Learning", "China"),
      (4, "Anna Kowalska", "Spark!", "Poland"),
    )).toDF("id", "user", TweetsSearch.TEXT, TweetsSearch.USER_LOCATION)

    // Keyword "Spark" should match "This is a tweet about Spark"
    val filteredDF = analyzer.searchByKeyWord("Spark")(data)

    // Assert expected number of rows
    assert(filteredDF.count() === 2)

  }

  // Test method searchByKeyWords
  test("searchByKeyWords should filter tweets containing all keywords") {

    val data = spark.createDataFrame(Seq(
      (1, "John Doe", "This is a tweet about Spark and Scala", "US"),
      (2, "Jane Smith", "Another tweet on Scala", "UK"),
      (3, "Li Wang", "Interesting article on Machine Learning", "China"),
      (4, "Anna Kowalska", "Spark !", "Poland")
    )).toDF("id", "user" , TweetsSearch.TEXT, TweetsSearch.USER_LOCATION)

    val filteredDF = analyzer.searchByKeyWords(Seq("Spark","Scala"))(data) // Keywords "Spark" and "Scala"


    // Assert expected number of rows (should find the tweet with keywords "Spark" or "Scala")
    assert(filteredDF.count() === 3)

  }

  // Test method onlyInLocation
  test("onlyInLocation should filter tweets from specific location") {

    val data = spark.createDataFrame(Seq(
      (1, "John Doe", "This is a tweet", "US"),
      (2, "Jane Smith", "Another tweet", "UK"),
      (3, "Li Wang", "Interesting article", "China"),
      (4, "Anna Kowalska", "Hello world!", "Poland"),
      (5, "John Doe", "Wow!", "US")
    )).toDF("id", "user" , TweetsSearch.TEXT, TweetsSearch.USER_LOCATION)

    val filteredDF = analyzer.onlyInLocation("US")(data)

    // Assert expected number of rows
    assert(filteredDF.count() === 2)

  }


}
