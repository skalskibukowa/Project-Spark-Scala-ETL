package analysers

import org.apache.spark.sql.functions.{col, desc, explode, split}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object TweetsUserAnalyzer {
  val TWEET_ID: String = "Tweet_ID"
  val USERNAME: String = "Username"
  val TEXT: String = "Text"
  val RETWEETS: String = "Retweets"
  val LIKES: String = "Likes"
  val DATE: String = "Date"
}
class TweetsUserAnalyzer(sparkSession: SparkSession) {

  /*
   Filter
   @param df
   @return Dataframe with columns Tweet_ID, Username, Text, Retweets, Likes, Date

   Description: Top 10 retweeted posts
   */

  def top10RetweetedPost(df: Dataset[Row]): Dataset[Row] = {
    df.select(TweetsUserAnalyzer.TWEET_ID,
      TweetsUserAnalyzer.USERNAME,
      TweetsUserAnalyzer.TEXT,
      TweetsUserAnalyzer.RETWEETS,
      TweetsUserAnalyzer.LIKES,
      TweetsUserAnalyzer.DATE)
      .orderBy(desc(TweetsUserAnalyzer.RETWEETS))
      .limit(10)
  }


  /*
   Filter
   @param df
   @return Dataframe with columns Tweet_ID, Username, Text, Retweets, Likes, Date

   Description: Top 10 liked posts
 */
  def top10LikedPosts(df: Dataset[Row]): Dataset[Row] = {
    df.select(TweetsUserAnalyzer.TWEET_ID,
        TweetsUserAnalyzer.USERNAME,
        TweetsUserAnalyzer.TEXT,
        TweetsUserAnalyzer.RETWEETS,
        TweetsUserAnalyzer.LIKES,
        TweetsUserAnalyzer.DATE)
      .orderBy(desc(TweetsUserAnalyzer.LIKES))
      .limit(10)
  }


  /*
   Aggregation
   @param df
   @return Dataframe with columns Username, count

   Description: Count number of posts posted by the users
 */

  def countUserPosts(df: Dataset[Row]): Dataset[Row] = {
    df.groupBy(TweetsUserAnalyzer.USERNAME)
      .count()
  }

  /*
  Aggregation
  @param df
  @return Dataframe with columns Text, count

  Description: Count number of posts posted by the users
*/
 def countWordsInTextColumn(df: Dataset[Row]): Dataset[Row] = {
   df.withColumn("Word", explode(split(col(TweetsUserAnalyzer.TEXT), "\\s+")))
     .groupBy("Word")
     .count()
     .orderBy(desc("count"))
 }
}
