package analysers

import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object TransactionAnalyzer {
  val COUNTRY: String = "Country"
}

class TransactionAnalyzer(sparkSession: SparkSession) {

  /*
    Filter by Country United States
    @param df
    @return Dataframe with columns ID, first_name,last_name, email, gender, Quantity, Value, Currency,Product,Credit_Card, Country,Postal_Code,Street_Address

    Description: Show United States Clients
  */
    def filterClientsUSA(df: Dataset[Row]): Dataset[Row] = {
      df.filter(col(ForbesAnalyzer.COUNTRY) === "United States")
    }

  /*
  Filter by Country China
  @param df
  @return Dataframe with columns ID, first_name,last_name, email, gender, Quantity, Value, Currency,Product,Credit_Card, Country,Postal_Code,Street_Address

  Description: Show China Clients
*/
  def filterClientsChina(df: Dataset[Row]): Dataset[Row] = {
    df.filter(col(ForbesAnalyzer.COUNTRY) === "China")
  }

  /*
 Filter by Country Poland
 @param df
 @return Dataframe with columns ID, first_name,last_name, email, gender, Quantity, Value, Currency,Product,Credit_Card, Country,Postal_Code,Street_Address

 Description: Show Poland Clients
*/
  def filterClientsPoland(df: Dataset[Row]): Dataset[Row] = {
    df.filter(col(ForbesAnalyzer.COUNTRY) === "Poland")
  }

/*
Filter by Country France
@param df
@return Dataframe with columns ID, first_name,last_name, email, gender, Quantity, Value, Currency,Product,Credit_Card, Country,Postal_Code,Street_Address

Description: Show France Clients
*/
  def filterClientsFrance(df: Dataset[Row]): Dataset[Row] = {
    df.filter(col(ForbesAnalyzer.COUNTRY) === "France")
  }


  /*
Filter top 10 Countries with most number of occurrence
@param df
@return Dataframe with columns ID, first_name,last_name, email, gender, Quantity, Value, Currency,Product,Credit_Card, Country,Postal_Code,Street_Address

Description: Show Top 10 Countries with most occurrence
*/
  def filterTop10ClientsCount(df: Dataset[Row]): Dataset[Row] = {
    df.groupBy(col(ForbesAnalyzer.COUNTRY))
      .count().as("Count")
      .orderBy(desc("Count"))
      .limit(10)
  }

}