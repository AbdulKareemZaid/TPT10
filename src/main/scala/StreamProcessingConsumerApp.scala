import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import java.util.Properties
import java.time.Duration
import scala.collection.JavaConverters._

object StreamProcessingConsumerApp {
  def start(spark: SparkSession): Unit = {
    import spark.implicits._

    val tweetSchema = new StructType()
      .add("created_at", StringType)
      .add("id_str", StringType)
      .add("text", StringType)
      .add("entities", new StructType()
        .add("hashtags", ArrayType(new StructType().add("text", StringType))))
      .add("geo", new StructType()
        .add("coordinates", ArrayType(DoubleType)))

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(java.util.Arrays.asList("twitter_topic"))

    while (true) {
      println("Polling for new records...")
      val records = consumer.poll(Duration.ofMillis(1000)).asScala

      if (records.nonEmpty) {
        println(s"Polled ${records.size} records")
        val tweets = records.map(_.value()).toList

        val tweetsDF = spark.createDataset(tweets).toDF("json")
          .select(from_json(col("json"), tweetSchema).as("data"))
          .select("data.*")

        val processedDF = tweetsDF
          .withColumn("cleaned_text", regexp_replace(col("text"), """http\S+|@\S+|#\S+""", " "))
          .withColumn("hashtags", col("entities.hashtags.text"))
          .withColumn("coordinates", concat_ws(", ", col("geo.coordinates")))
          .withColumn("timestamp", to_timestamp(col("created_at"), "EEE MMM dd HH:mm:ss Z yyyy"))
          .na.drop("any", Seq("timestamp"))

        val sentimentDetector = PretrainedPipeline("analyze_sentimentdl_glove_imdb", lang = "en")

        val sentimentDF = sentimentDetector.transform(processedDF)

        val sentimentResultDF = sentimentDF
          .select("cleaned_text", "hashtags", "coordinates", "timestamp", "sentiment.result")

        val sentimentToValue = udf((sentiments: Seq[String]) => {
          sentiments.flatMap(_.split(",")).map(_.trim.stripPrefix("[").stripSuffix("]") match {
            case "pos" => 1
            case "neg" => -1
            case _ => 0
          }).sum
        })

        val scoredDF = sentimentResultDF.withColumn("sentiment_score", sentimentToValue(col("result")))

        scoredDF.collect().foreach { row =>
          println(s"Processed row: $row")
        }

        // here you can add code to write the processed data to another Kafka topic or further process it as needed

      } else {
        println("No new records to process.")
      }
    }
  }
}
