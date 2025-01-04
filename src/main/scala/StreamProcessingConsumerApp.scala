import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import java.util.Properties
import java.time.Duration
import scala.collection.JavaConverters._
import java.util.concurrent.{Executors, TimeUnit}

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

    val sentimentDetector = PretrainedPipeline("analyze_sentimentdl_glove_imdb",
      lang = "en",
      diskLocation = Some("analyze"))

    val pollingTask = new Runnable {
      override def run(): Unit = {
        println("Polling for new records...")
        val records = consumer.poll(Duration.ofMillis(1000)).asScala
        println(s"Fetched ${records.size} records")

        if (records.nonEmpty) {
          println(s"Polled ${records.size} records")
          val tweets = records.map(_.value()).toList
          println(s"Extracted tweets: $tweets")

          val preprocessedTweets = tweets.map { tweet =>
            try {
              val fields = tweet.split(",", -1)
              if (fields.length >= 5) {
                val json =
                  s"""
                     |{
                     |  "created_at": "${fields(0)}",
                     |  "id_str": "${fields(1)}",
                     |  "text": "${fields(2)}",
                     |  "entities": {
                     |    "hashtags": [{"text": "${fields(3).replace("[WrappedArray(", "").replace(")]", "").split(",").map(_.trim).mkString("\"}, {\"text\": \"")}" }]
                     |  },
                     |  "geo": ${if (fields(4) != "null") s"""{"coordinates": [${fields(4)}]}""" else "null"}
                     |}
                  """.stripMargin
                json
              } else {
                println(s"Invalid data: $tweet")
                null
              }
            } catch {
              case e: Exception =>
                println(s"Error processing tweet: $tweet. Error: ${e.getMessage}")
                null
            }
          }.filter(_ != null)

          val tweetsDF = spark.createDataset(preprocessedTweets).toDF("json")
            .select(from_json(col("json"), tweetSchema).as("data"))
            .select("data.*")
          tweetsDF.show(false)

          val processedDF = tweetsDF
            .withColumn("cleaned_text", regexp_replace(col("text"), """http\S+|@\S+|#\S+""", " "))
            .withColumn("hashtags", col("entities.hashtags.text"))
            .withColumn("coordinates", concat_ws(", ", col("geo.coordinates")))
            .withColumn("timestamp", to_timestamp(col("created_at"), "EEE MMM dd HH:mm:ss Z yyyy"))
            .na.drop("any", Seq("timestamp"))
          processedDF.show(false)

          val sentimentDF = sentimentDetector.transform(processedDF)
          sentimentDF.show(false)

          val sentimentResultDF = sentimentDF
            .select("cleaned_text", "hashtags", "coordinates", "timestamp", "sentiment.result")
          sentimentResultDF.show(false)

          val sentimentToValue = udf((sentiments: Seq[String]) => {
            sentiments.flatMap(_.split(",")).map(_.trim.stripPrefix("[").stripSuffix("]") match {
              case "pos" => 1
              case "neg" => -1
              case _ => 0
            }).sum
          })

          val scoredDF = sentimentResultDF.withColumn("sentiment_score", sentimentToValue(col("result")))
          scoredDF.show(false)

          println(s"scoredDF has ${scoredDF.count()} rows")
          scoredDF.collect().foreach { row =>
            println(s"Processed row: $row")
            val data = Map(
              "cleaned_text" -> row.getAs[String]("cleaned_text"),
              "hashtags" -> Option(row.getAs[Seq[String]]("hashtags")).getOrElse(Seq.empty).mkString(", "),
              "coordinates" -> Option(row.getAs[String]("coordinates")).getOrElse(""),
              "timestamp" -> Option(row.getAs[java.sql.Timestamp]("timestamp")).map(_.toString).getOrElse(""),
              "sentiment_score" -> row.getAs[Int]("sentiment_score").toString
            )
            Database.insertDocument(data)
          }
        } else {
          println("No new records to process.")
        }
      }
    }
    Database.close()

    val scheduler = Executors.newScheduledThreadPool(1)
    scheduler.scheduleAtFixedRate(pollingTask, 0, 1, TimeUnit.SECONDS)
  }
}
