package UnitTests.cleaners

import cleaners.TransactionCleaner
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TransactionCleanerTest extends AnyFunSuite with BeforeAndAfterAll {


  private var spark: SparkSession = _
  private var transactionCleaner: TransactionCleaner = _
  private var inputDF: DataFrame = _


  override def beforeAll(): Unit = {

    spark = SparkSession
      .builder()
      .appName("TransactionCleanerTest")
      .master("local[*]")
      .getOrCreate()

    transactionCleaner = new TransactionCleaner(spark)

    // Input data
    inputDF = spark.createDataFrame(Seq(
      (" 123 ", "  John ", " Doe ", " john.doe@example.com ", "  Male  ", " USD ", "   Book   ", " US ", "  12345 ", "  Kobierzynska  "),
      ("456", "Jane", " Doe", "jane.doe@example.com", "Female", "EUR", "Pen", "FR", "54321", "Another Street")
    )).toDF("ID", "first_name", "last_name", "email", "gender", "Currency", "Product", "Country", "Postal_Code", "Street_Address")

  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("should trim leading and trailing whitespaces from all string columns") {

    val cleanedDF = transactionCleaner.cleanAllTransactions(inputDF)

    assert(cleanedDF.select("ID").head().getAs[String]("ID") === "123")
    assert(cleanedDF.select("first_name").head().getAs[String]("first_name") === "John")
    assert(cleanedDF.select("last_name").head().getAs[String]("last_name") === "Doe")
    assert(cleanedDF.select("email").head().getAs[String]("email") === "john.doe@example.com")
    assert(cleanedDF.select("gender").head().getAs[String]("gender") === "Male")
    assert(cleanedDF.select("Currency").head().getAs[String]("Currency") === "USD")
    assert(cleanedDF.select("Product").head().getAs[String]("Product") === "Book")
    assert(cleanedDF.select("Country").head().getAs[String]("Country") === "US")
    assert(cleanedDF.select("Postal_Code").head().getAs[String]("Postal_Code") === "12345")
    assert(cleanedDF.select("Street_Address").head().getAs[String]("Street_Address") === "Kobierzynska")
  }

}
