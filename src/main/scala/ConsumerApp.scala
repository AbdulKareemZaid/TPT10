import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object ConsumerApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ConsumerApp")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val tweetSchema = new StructType()
      .add("created_at", StringType)
      .add("id", StringType)
      .add("text", StringType)
      .add("geo", StringType)
      .add("coordinates", StringType)
      .add("place", StringType)
      .add("user", StringType)

    // Read stream from Kafka
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092") // Update with your Kafka broker address
      .option("subscribe", "twitter_topic") // Update with your Kafka topic
      .load()

    // Extract JSON data from Kafka value
    val tweetsDF = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]
      .select(from_json(col("value"),
        tweetSchema).as("data")).select("data.*")

    // Process the streaming data
    val query = tweetsDF.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Await termination
    query.awaitTermination()
  }
}
