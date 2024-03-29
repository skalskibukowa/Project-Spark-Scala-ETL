package UnitTests.analysers

import analysers.ForbesAnalyzer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, desc, lit}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ForbesAnalyzerTest extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var analyzer: ForbesAnalyzer = _


  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("ForbesCleaner-Test")
      .master("local[*]")
      .getOrCreate()

    analyzer = new ForbesAnalyzer(spark)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }


  test("filterUnderFiftyAgeForbes should filter richest under 50") {
    val data = spark.createDataFrame(Seq(
      (1, "Bill Gates", 45, 100.0, "USA", "Forbes"),
      (2, "Warren Buffett", 80, 90.0, "USA", "Forbes"),
      (3, "Mark Zuckerberg", 38, 80.0, "USA", "Forbes"),
      (4, "Mukesh Ambani", 65, 70.0, "India", "Forbes"),
    )).toDF(ForbesAnalyzer.RANK, ForbesAnalyzer.PERSON_NAME, ForbesAnalyzer.AGE, ForbesAnalyzer.FINAL_WORTH, ForbesAnalyzer.COUNTRY, ForbesAnalyzer.SOURCE)

    val filteredDF = analyzer.filterUnderFiftyAgeForbes(data)

    // Assert expected number of rows

    assert(filteredDF.count() === 2)

    // Assert specific person under 50
    assert(filteredDF.filter(col(ForbesAnalyzer.PERSON_NAME) === "Mark Zuckerberg").count() === 1)

  }


  test("countCountriesForbes should count richest guys by country") {

    val data = spark.createDataFrame(Seq(
      (1, "Bill Gates", 45, 100.0, "USA", "Forbes"),
      (2, "Warren Buffett", 80, 90.0, "USA", "Forbes"),
      (3, "Mark Zuckerberg", 38, 80.0, "USA", "Forbes"),
      (4, "Mukesh Ambani", 65, 70.0, "India", "Forbes"),
    )).toDF(ForbesAnalyzer.RANK, ForbesAnalyzer.PERSON_NAME, ForbesAnalyzer.AGE, ForbesAnalyzer.FINAL_WORTH, ForbesAnalyzer.COUNTRY, ForbesAnalyzer.SOURCE)

    val countryCountsDF = analyzer.countCountriesForbes(data)

    // Assert expected countries
    countryCountsDF.show()
    assert(countryCountsDF.filter(col(ForbesAnalyzer.COUNTRY) === "USA").head().getAs[Int](ForbesAnalyzer.COUNTRY_NO) === 3)
    assert(countryCountsDF.filter(col(ForbesAnalyzer.COUNTRY) === "India").head().getAs[Int](ForbesAnalyzer.COUNTRY_NO) === 1)

    // Assert ordering by count descending
    val sortedCountriesDesc = countryCountsDF.sort(desc(ForbesAnalyzer.COUNTRY_NO))
    assert(sortedCountriesDesc.head.getAs[String](ForbesAnalyzer.COUNTRY) === "USA")

    val sortedCountriesAsc = countryCountsDF.sort(asc(ForbesAnalyzer.COUNTRY_NO))
    assert(sortedCountriesAsc.head.getAs[String](ForbesAnalyzer.COUNTRY) === "India")
  }

  test("top10SelfMade should show top 10 self-made people") {

    val data = spark.createDataFrame(Seq(
      (1, "Bill Gates", 45, 100.0, lit("True"), "Forbes"),
      (2, "Warren Buffett", 80, 90.0, lit("False"), "Forbes"),
      (3, "Mark Zuckerberg", 38, 80.0, lit("True"), "Forbes"),
      (4, "Mukesh Ambani", 65, 70.0, lit("True"), "Forbes"),
      (5, "Jeff Bezos", 52, 110.0, lit("True"), "Forbes"),
      (6, "Larry Ellison", 78, 60.0, lit("True"), "Forbes"),
      (7, "Charles Koch", 86, 50.0, lit("False"), "Forbes"),
      (8, "David Koch", 86, 50.0, lit("False"), "Forbes"),
      (9, "Michael Bloomberg", 79, 50.0, lit("True"), "Forbes"),
      (10, "Larry Page", 49, 48.0, lit("True"), "Forbes"),
      (11, "Ricky Morris", 20, 50.0, lit("True"), "Forbes"),
      (13, "Wart Lomn", 40, 50.0, lit("True"), "Forbes"),
      (14, "Draws Dsaim", 30, 48.0, lit("True"), "Forbes"),
      (15, "Vericks Flas", 20, 50.0, lit("True"), "Forbes")
    )).toDF(ForbesAnalyzer.RANK, ForbesAnalyzer.PERSON_NAME, ForbesAnalyzer.AGE, ForbesAnalyzer.FINAL_WORTH, ForbesAnalyzer.SELF_MADE, ForbesAnalyzer.SOURCE)


  }
}
