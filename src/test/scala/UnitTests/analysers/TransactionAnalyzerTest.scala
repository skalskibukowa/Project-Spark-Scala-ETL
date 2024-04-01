package UnitTests.analysers

import analysers.TransactionAnalyzer
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TransactionAnalyzerTest extends AnyFunSuite with BeforeAndAfterAll{


  private var spark: SparkSession = _
  private var analyzer: TransactionAnalyzer = _


  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("TransactionAnalyzer-Test")
      .master("local[*]")
      .getOrCreate()


    analyzer = new TransactionAnalyzer(spark)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }


  // Test method filterClientsUSA
  test("filterClientsUSA should filter clients from United States") {

    val data = spark.createDataFrame(Seq(
      (1, "John", "Doe", "john.doe@example.com", "Male", 1, 100.0, "USD", "Product A", "1234567890123456", "United States", "12345", "Street 1"),
      (2, "Jane", "Smith", "jane.smith@example.com", "Female", 2, 50.0, "EUR", "Product B", "9876543210987654", "France", "54321", "Street 2"),
      (3, "Li", "Wang", "li.wang@example.com", "Male", 3, 200.0, "CNY", "Product C", "0123456789012345", "China", "98765", "Street 3"),
    )).toDF("ID", "first_name", "last_name", "email", "gender", "Quantity", "Value", "Currency", "Product", "Credit_Card", TransactionAnalyzer.COUNTRY, "Postal_Code", "Street_Address")

    val filteredDF = analyzer.filterClientsUSA(data)

    // Assert expected number of rows for USA
    assert(filteredDF.count() === 1)

  }


  // Test method filterClientsChina
  test("filterClientsChina should filter clients from China") {

    val data = spark.createDataFrame(Seq(
      (1, "John", "Doe", "john.doe@example.com", "Male", 1, 100.0, "USD", "Product A", "1234567890123456", "United States", "12345", "Street 1"),
      (2, "Jane", "Smith", "jane.smith@example.com", "Female", 2, 50.0, "EUR", "Product B", "9876543210987654", "France", "54321", "Street 2"),
      (3, "Li", "Wang", "li.wang@example.com", "Male", 3, 200.0, "CNY", "Product C", "0123456789012345", "China", "98765", "Street 3"),
    )).toDF("ID", "first_name", "last_name", "email", "gender", "Quantity", "Value", "Currency", "Product", "Credit_Card", TransactionAnalyzer.COUNTRY, "Postal_Code", "Street_Address")

    val filteredDF = analyzer.filterClientsChina(data)

    // Assert expected number of rows for China
    assert(filteredDF.count() === 1)

  }

  // Test method filterClientsPoland
  test("filterClientsPoland should filter clients from Poland") {

    val data = spark.createDataFrame(Seq(
      (1, "John", "Doe", "john.doe@example.com", "Male", 1, 100.0, "USD", "Product A", "1234567890123456", "United States", "12345", "Street 1"),
      (2, "Jane", "Smith", "jane.smith@example.com", "Female", 2, 50.0, "EUR", "Product B", "9876543210987654", "Poland", "54321", "Street 2"),
      (3, "Li", "Wang", "li.wang@example.com", "Male", 3, 200.0, "CNY", "Product C", "0123456789012345", "China", "98765", "Street 3"),
    )).toDF("ID", "first_name", "last_name", "email", "gender", "Quantity", "Value", "Currency", "Product", "Credit_Card", TransactionAnalyzer.COUNTRY, "Postal_Code", "Street_Address")

    val filteredDF = analyzer.filterClientsPoland(data)

    // Assert expected number of rows for Poland
    assert(filteredDF.count() === 1)


  }

  // Test method filterClientsFrance
  test("filterClientsFrance should filter clients from Poland") {

    val data = spark.createDataFrame(Seq(
      (1, "John", "Doe", "john.doe@example.com", "Male", 1, 100.0, "USD", "Product A", "1234567890123456", "United States", "12345", "Street 1"),
      (2, "Jane", "Smith", "jane.smith@example.com", "Female", 2, 50.0, "EUR", "Product B", "9876543210987654", "France", "54321", "Street 2"),
      (3, "Li", "Wang", "li.wang@example.com", "Male", 3, 200.0, "CNY", "Product C", "0123456789012345", "France", "98765", "Street 3"),
    )).toDF("ID", "first_name", "last_name", "email", "gender", "Quantity", "Value", "Currency", "Product", "Credit_Card", TransactionAnalyzer.COUNTRY, "Postal_Code", "Street_Address")

    val filteredDF = analyzer.filterClientsFrance(data)

    // Assert expected number of rows for France
    assert(filteredDF.count() === 2)

  }

}
