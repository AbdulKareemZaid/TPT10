import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    System.setProperty("log4j.configuration", "file:src/main/resources/log4j.properties")

    val conf = new SparkConf()
      .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .set("spark.mongodb.write.connection.uri", "mongodb+srv://teamtp10:test123@cluster0.5jb5h.mongodb.net")
      .set("spark.mongodb.write.database", "twitter_db")
      .set("spark.mongodb.write.collection", "tweets")

    val spark = SparkSession.builder()
      .appName("TweetProcessingSession")
      .master("local[*]")
      .config(conf)
      .getOrCreate()

    val consumerThread = new Thread(() => {
      Consumer.run(spark)
    })
    val producerThread = new Thread(() => {
      Producer.start(spark)
    })

    consumerThread.start()
    producerThread.start()

    consumerThread.join()
    producerThread.join()

    spark.streams.awaitAnyTermination()
  }
}
