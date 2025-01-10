import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Producer {
  def start(spark: SparkSession): Unit = {


    import spark.implicits._


    val staticDf = spark.read.json("data/boulder_flood_geolocated_tweets.json")


    val windowSpec = Window.orderBy("retweet_count")
    val staticDfWithIndex = staticDf.withColumn("index", row_number().over(windowSpec) - 1)


    val rateStreamDf = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 1) // 1 row per second
      .load()
      .selectExpr("value as index")


    val hashtagsDf = staticDf.withColumn("hashtags", expr("entities.hashtags.text"))


    val streamingDf1 = rateStreamDf.join(staticDfWithIndex, "index")


    val streamingDf = streamingDf1.join(hashtagsDf.select("id", "hashtags"), "id")
      .withColumn("cleaned_text", regexp_replace(col("text"), """http\S+|@\S+|#\S+""", ""))
      .withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM dd HH:mm:ss ZZZ yyyy"))


    val finalDf = streamingDf.select("created_at", "geo.coordinates", "hashtags", "cleaned_text")



    val finalDfWithValue = finalDf.withColumn("value", to_json(struct($"*")))


    val query = finalDfWithValue.selectExpr("CAST(value AS STRING) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "twitter-topic")
      .option("checkpointLocation", "tmp/checkPointTweets")
      .outputMode("append")
      .start()


    query.awaitTermination()
  }
}
