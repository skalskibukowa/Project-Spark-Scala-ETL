package analysers

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object TransactionSearch{
  val COUNTRY: String = "Country"
  val CURRENCY: String = "Currency"
}
class TransactionSearch(sparkSession: SparkSession) {

  def searchByCountry(keyWord: String)(df: Dataset[Row]): Dataset[Row]={
    df.filter(col(TransactionSearch.COUNTRY).contains(keyWord))
  }
  def searchByCurrency(keyWord: String)(df: Dataset[Row]): Dataset[Row]={
    df.filter(col(TransactionSearch.CURRENCY).contains(keyWord))
  }

}
