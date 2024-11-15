package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

/** Tracks the most frequently mentioned hashtags on Twitter in real-time,
 *  using a 5-minute sliding window. 
 */
object TwitterTrendingHashtags {

  /** Reduces logging noise to show only ERROR messages (nobody wants a log flood). */
  def minimizeLogs(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  /** Loads Twitter API keys from a local config file (twitter.txt). */
  def configureTwitter(): Unit = {
    import scala.io.Source
    for (line <- Source.fromFile("../twitter.txt").getLines()) {
      val tokens = line.split(" ")
      if (tokens.length == 2) {
        System.setProperty("twitter4j.oauth." + tokens(0), tokens(1))
      }
    }
  }

  /** Main driver code for hashtag tracking using Spark Streaming. */
  def main(args: Array[String]): Unit = {

    // Set up Twitter credentials (must have twitter.txt configured correctly).
    configureTwitter()

    // Initialize a Spark streaming context
    val streamingContext = new StreamingContext("local[*]", "TwitterTrendingHashtags", Seconds(1))

    // Suppress unwanted logs after context initialization
    minimizeLogs()

    // Create a DStream to capture live tweets
    val liveTweets = TwitterUtils.createStream(streamingContext, None)

    // Extract the tweet text from each status update
    val tweetTexts = liveTweets.map(tweet => tweet.getText)

    // Break the tweets into words
    val words = tweetTexts.flatMap(text => text.split(" "))

    // Keep only words that are hashtags
    val hashtags = words.filter(word => word.startsWith("#"))

    // Create (hashtag, 1) pairs for counting
    val hashtagPairs = hashtags.map(tag => (tag, 1))

    // Count hashtags over a 5-minute window with a 1-second slide
    val hashtagCounts = hashtagPairs.reduceByKeyAndWindow(
      (x, y) => x + y,
      (x, y) => x - y,
      Seconds(300),
      Seconds(1)
    )

    // Sort hashtags by their counts
    val sortedHashtags = hashtagCounts.transform(rdd => rdd.sortBy(hashtag => hashtag._2, ascending = false))

    // Show the top 10 hashtags in real-time
    sortedHashtags.print(10)

    // Set checkpoint directory and start streaming
    streamingContext.checkpoint("checkpoints/")
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
