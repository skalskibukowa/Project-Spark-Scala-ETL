package analysers

import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object ForbesAnalyzer {
  val RANK: String = "rank"
  val PERSON_NAME: String = "personName"
  val AGE: String = "age"
  val FINAL_WORTH: String = "finalWorth"
  val SOURCE: String = "source"
  val COUNTRY: String = "country"
  val COUNTRY_NO: String = "country_no"
  val SELF_MADE: String = "selfMade"
}


class ForbesAnalyzer(sparkSession: SparkSession) {

  /*
   Filter
   @param df
   @return Dataframe with columns rank, personName, age, finalWorth, country, source

   Description: Filter richest guys under fifty years old
   */

  def filterUnderFiftyAgeForbes(df: Dataset[Row]): Dataset[Row] = {
    df.select(ForbesAnalyzer.RANK, ForbesAnalyzer.PERSON_NAME, ForbesAnalyzer.AGE, ForbesAnalyzer.COUNTRY,  ForbesAnalyzer.SOURCE, ForbesAnalyzer.FINAL_WORTH)
      .filter(col(ForbesAnalyzer.AGE) < 50)
  }

  /*
  Aggregation
  @param df
  @return Dataframe with columns country, count

  Description: Show the number of richest guys in countries
   */

  def countCountriesForbes(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn(ForbesAnalyzer.COUNTRY_NO, col(ForbesAnalyzer.COUNTRY))
      .groupBy(ForbesAnalyzer.COUNTRY)
      .count()
      .withColumnRenamed("count", ForbesAnalyzer.COUNTRY_NO)
      .orderBy(desc(ForbesAnalyzer.COUNTRY_NO))
  }

  /*
  Aggregation
  @param df
  @return Dataframe with columns rank, personName, age, finalWorth

  Description: Show top 10 self-made
   */

  def top10SelfMade(df: Dataset[Row]): Dataset[Row] = {
    df.select(col(ForbesAnalyzer.RANK), col(ForbesAnalyzer.PERSON_NAME), col(ForbesAnalyzer.AGE), col(ForbesAnalyzer.FINAL_WORTH))
      .filter(col(ForbesAnalyzer.SELF_MADE) === "True")
      .sort(col(ForbesAnalyzer.SELF_MADE).desc)
      .limit(10)
  }
}
