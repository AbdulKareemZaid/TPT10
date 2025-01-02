import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.sql.types.{StringType, StructType}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Properties

object ProducerApp {
  def main(args: Array[String]): Unit = {
    System.setProperty("log4j.configuration", "file:src/main/resources/log4j.properties")

    val spark = SparkSession.builder()
      .appName("ProducerApp")
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
    kafkaProps.put("bootstrap.servers", "localhost:9092")
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
          val dateTime = Instant.ofEpochMilli(metadata.timestamp())
            .atZone(ZoneId.systemDefault())
            .format(DateTimeFormatter.ofPattern("HH:mm:ss"))
          println(s"Message delivered to ${metadata.topic()} [${metadata.partition()}] at $dateTime")
        }
      })

      producer.flush()
      Thread.sleep(1000)
    }

    producer.close()
    spark.stop()

    println("Producer finished sending messages.")
  }
}
