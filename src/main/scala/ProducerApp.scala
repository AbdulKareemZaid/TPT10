import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.sql.types.{StringType, StructType}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper

import java.util.Properties

object ProducerApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaSparkProducer")
      .master("local[*]")
      .getOrCreate()

    println("Starting Producer...")

    val filePath = "./data/boulder_flood_geolocated_tweets.json"
    val tweetSchema = new StructType()
      .add("created_at", StringType)
      .add("id", StringType)
      .add("text", StringType)
      .add("geo", StringType)
      .add("coordinates", StringType)
      .add("place", StringType)
      .add("user", StringType)

    val tweetDF: DataFrame = spark.read.schema(tweetSchema).json(filePath)

    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "192.168.1.98:9092")
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](kafkaProps)
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)

    tweetDF.collect().foreach { row =>
      val tweetJson = objectMapper.writeValueAsString(row.getValuesMap(row.schema.fieldNames))
      val record = new ProducerRecord[String, String]("twitter_topic", row.getAs[String]("id"), tweetJson)

      producer.send(record, (metadata: RecordMetadata, exception: Exception) => {
        if (exception != null) {
          println(s"Message delivery failed: ${exception.getMessage}")
        } else {
          println(s"Message delivered to ${metadata.topic()} [${metadata.partition()}]")
        }
      })

      producer.flush()
      Thread.sleep(1000)  // Add delay between messages to simulate streaming
    }

    producer.close()
    spark.stop()

    println("Producer finished sending messages.")
  }
}
