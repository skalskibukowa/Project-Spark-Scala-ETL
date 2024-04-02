package UnitTests.analysers

import analysers.TransactionSearch
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TransactionSearchTest extends AnyFunSuite with BeforeAndAfterAll{

  private var spark: SparkSession = _
  private var analyzer: TransactionSearch = _


  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("TransactionSearch-Test")
      .master("local[*]")
      .getOrCreate()

    analyzer = new TransactionSearch(spark)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }


  // Test method searchByCountry

  test("searchByCountry should filter transactions by country containing keyword") {

    val data = spark.createDataFrame(Seq(
      (1, "John", "Doe", "john.doe@example.com", "Male", 1, 100.0, "USD", "Product A", "1234567890123456", "USA", "12345", "Street 1"),
      (2, "Jane", "Smith", "jane.smith@example.com", "Female", 2, 50.0, "EUR", "Product B", "9876543210987654", "France", "54321", "Street 2"),
      (3, "Li", "Wang", "li.wang@example.com", "Male", 3, 200.0, "CNY", "Product C", "0123456789012345", "China", "98765", "Street 3"),
      (4, "John", "Doe", "john.doe@example.com", "Male", 1, 100.0, "USD", "Product A", "1234567890123456", "USA", "12345", "Street 1"),
      (5, "Jane", "Smith", "jane.smith@example.com", "Female", 2, 50.0, "EUR", "Product B", "9876543210987654", "Luxembourg", "54321", "Street 2"),
      (6, "Li", "Wang", "li.wang@example.com", "Male", 3, 200.0, "CNY", "Product C", "0123456789012345", "China", "98765", "Street 3"),
      (7, "John", "Doe", "john.doe@example.com", "Male", 1, 100.0, "USD", "Product A", "1234567890123456", "Brazil", "12345", "Street 1"),
      (8, "Jane", "Smith", "jane.smith@example.com", "Female", 2, 50.0, "EUR", "Product B", "9876543210987654", "Paraguay", "54321", "Street 2"),
      (9, "Li", "Wang", "li.wang@example.com", "Male", 3, 200.0, "CNY", "Product C", "0123456789012345", "China", "98765", "Street 3")
    )).toDF("ID", "first_name", "last_name", "email", "gender", "Quantity", "Value", "Currency", "Product", "Credit_Card", TransactionSearch.COUNTRY, "Postal_Code", "Street_Address")

    val filteredDF = analyzer.searchByCountry("China")(data)


    // Should return two records
    assert(filteredDF.count() === 3)

  }


  test("searchByCurrency should filter transactions by currency containing keyword") {

    val data = spark.createDataFrame(Seq(
      (1, "John", "Doe", "john.doe@example.com", "Male", 1, 100.0, "EUR", "Product A", "1234567890123456", "USA", "12345", "Street 1"),
      (2, "Jane", "Smith", "jane.smith@example.com", "Female", 2, 50.0, "EUR", "Product B", "9876543210987654", "France", "54321", "Street 2"),
      (3, "Li", "Wang", "li.wang@example.com", "Male", 3, 200.0, "EUR", "Product C", "0123456789012345", "China", "98765", "Street 3"),
      (4, "John", "Doe", "john.doe@example.com", "Male", 1, 100.0, "EUR", "Product A", "1234567890123456", "USA", "12345", "Street 1"),
      (5, "Jane", "Smith", "jane.smith@example.com", "Female", 2, 50.0, "EUR", "Product B", "9876543210987654", "Luxembourg", "54321", "Street 2"),
      (6, "Li", "Wang", "li.wang@example.com", "Male", 3, 200.0, "CNY", "Product C", "0123456789012345", "China", "98765", "Street 3"),
      (7, "John", "Doe", "john.doe@example.com", "Male", 1, 100.0, "USD", "Product A", "1234567890123456", "Brazil", "12345", "Street 1"),
      (8, "Jane", "Smith", "jane.smith@example.com", "Female", 2, 50.0, "EUR", "Product B", "9876543210987654", "Paraguay", "54321", "Street 2"),
      (9, "Li", "Wang", "li.wang@example.com", "Male", 3, 200.0, "CNY", "Product C", "0123456789012345", "China", "98765", "Street 3")
    )).toDF("ID", "first_name", "last_name", "email", "gender", "Quantity", "Value", "Currency", "Product", "Credit_Card", TransactionSearch.COUNTRY, "Postal_Code", "Street_Address")

    val filteredDF = analyzer.searchByCurrency("EUR")(data)


    // Should return six records
    assert(filteredDF.count() === 6)

  }

}
