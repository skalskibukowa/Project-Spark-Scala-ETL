package UnitTests.cleaners

import cleaners.ForbesCleaner
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ForbesCleanerTest extends AnyFunSuite with BeforeAndAfterAll{

  private var spark: SparkSession = _
  private var forbesCleaner: ForbesCleaner = _
  private var inputDF: DataFrame = _


  override def beforeAll(): Unit = {

    spark = SparkSession
      .builder()
      .appName("ForbesCleaner-Test")
      .master("local[*]")
      .getOrCreate()

    forbesCleaner = new ForbesCleaner(spark)

    // Input data
    inputDF = spark.createDataFrame(Seq(
      (123, "45", "67", 2023, 5, "2000-01-01", 0.5),
      (444, "35", "89", 2022, 6, "1995-02-15", 0.8),
      (333, "25", "42", 2021, 7, "1980-11-20", 0.3)
    )).toDF("rank", "age", "finalWorth", "year", "month", "birthDate", "philanthropyScore")

  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("should cast rank to integer, filter nulls, and cast other columns") {

    // cleanForbes function
    val cleanedDF = forbesCleaner.cleanForbes(inputDF)

    assert(cleanedDF.schema("rank").dataType.typeName === "IntegerType") // Check if rank column is of IntegerType
    assert(cleanedDF.schema("age").dataType.typeName === "IntegerType") // Check if age column is of IntegerType
    assert(cleanedDF.schema("finalWorth").dataType.typeName === "IntegerType") // Check if finalWorth column is of IntegerType
    assert(cleanedDF.schema("year").dataType.typeName === "IntegerType") // Check if year column is of IntegerType
    assert(cleanedDF.schema("month").dataType.typeName === "IntegerType") // Check if month column is of IntegerType
    assert(cleanedDF.schema("birthDate").dataType.typeName === "DateType") // Check if birthDate column is of DateType
    assert(cleanedDF.schema("philanthropyScore").dataType.typeName === "FloatType") // Check if philanthropyScore column is of FloatType
  }

}
