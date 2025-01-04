import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.types._
import java.util.Properties

object ProducerApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ProducerApp")
      .master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()

    // Define the schema for the JSON data
    val tweetSchema = new StructType()
      .add("created_at", StringType)
      .add("id_str", StringType)
      .add("text", StringType)
      .add("entities", new StructType()
        .add("hashtags", ArrayType(new StructType().add("text", StringType))))
      .add("geo", new StructType()
        .add("coordinates", ArrayType(DoubleType)))

    val df = spark.read.schema(tweetSchema).json("data/boulder_flood_geolocated_tweets.json")

    // producer properties
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // process and send each row to Kafka with a delay
    df.collect().foreach { row =>
      val key = row.getAs[String]("id_str")
      val value = row.mkString(",")
      println(s"Adding to topic: key = $key, value = $value")

      val record = new ProducerRecord[String, String]("twitter_topic", key, value)
      producer.send(record)

      // 2-second delay
    }

    producer.close()
    spark.stop()

    println("Producer finished sending messages.")
  }
}
