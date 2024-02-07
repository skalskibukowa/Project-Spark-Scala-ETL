package analysers

import org.apache.spark.sql.functions.{col, explode_outer}
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object TweetsAnalyzer {
  val HASHTAG_COLUMN: String = "hashtags"
  val IS_RETWEET_COLUMN: String = "is_retweet"
  val SOURCE_COLUMN: String = "source"
  val USER_FOLLOWERS: String = "user_followers"
  val USER_NAME: String = "user_name"
  val USER_LOCATION: String = "user_location"
}



class TweetsAnalyzer(sparkSession: SparkSession) {

  /*
    AGGREGATION
    @param df
    @return Dataframe with columns hashtag, count

    Description: Show number of hashtags
  */

  def calculateHashtags(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn(TweetsAnalyzer.HASHTAG_COLUMN, explode_outer(col(TweetsAnalyzer.HASHTAG_COLUMN)))
      .groupBy(TweetsAnalyzer.HASHTAG_COLUMN).count()
  }

  /*
  AGGREGATION
  @param df
  @return Dataframe with columns is_retweet, count

  Description: Show number of retweets
*/

  def calculateIsRetweetCount(df: Dataset[Row]): Dataset[Row]={
    df.groupBy(TweetsAnalyzer.IS_RETWEET_COLUMN).count()
  }

  /*
  AGGREGATION
  @param df
  @return Dataframe with columns source,count

  Description: Show number of sources
*/
  def calculateSourceCount(df: Dataset[Row]): Dataset[Row]={
    df.groupBy(TweetsAnalyzer.SOURCE_COLUMN).count()
  }

  /*
  AGGREGATION
  @param df
  @return Dataframe with columns user_location_avg

  Description: Show average user followers per location
*/

  def calculateAvgUserFollowersPerLocation(df: Dataset[Row]): Dataset[Row]={
    df.select(TweetsAnalyzer.USER_NAME, TweetsAnalyzer.USER_FOLLOWERS, TweetsAnalyzer.USER_LOCATION)
      .filter(col(TweetsAnalyzer.USER_NAME).isNotNull)
      .filter(col(TweetsAnalyzer.USER_LOCATION).isNotNull)
      .dropDuplicates(TweetsAnalyzer.USER_NAME)
      .groupBy(TweetsAnalyzer.USER_LOCATION)
      .avg(TweetsAnalyzer.USER_FOLLOWERS)
      .as("avg")
  }

}
