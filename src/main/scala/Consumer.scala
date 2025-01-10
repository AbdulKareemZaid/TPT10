import org.apache.spark.sql.SparkSession
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}

object Consumer {
  def run(spark: SparkSession): Unit = {

    val kafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "twitter-topic")
      .option("startingOffsets", "latest")
      .load()

    val schema = StructType(Array(
      StructField("created_at", StringType, nullable = false),
      StructField("coordinates", ArrayType(DoubleType), nullable = true),
      StructField("hashtags", ArrayType(StringType), nullable = true),
      StructField("cleaned_text", StringType, nullable = false)
    ))

    val tweetsDf = kafkaDf.selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), schema).as("data"))
      .select("data.*")


    val renamedTextDf = tweetsDf.withColumn("text", col("cleaned_text"))


    val sentimentDetector = PretrainedPipeline("analyze_sentimentdl_glove_imdb",
      lang = "en",
      diskLocation = Some("analyze")
    )

    val sentimentDf = sentimentDetector.transform(renamedTextDf).select("created_at", "coordinates", "hashtags", "text", "sentiment.result")


    val sentimentToValue = udf((sentiments: Seq[String]) => {
      sentiments.flatMap(_.split(",")).map(_.trim.stripPrefix("[").stripSuffix("]") match {
        case "pos" => 1
        case "neg" => -1
        case _ => 0
      }).sum
    })

    val scoredDf = sentimentDf.withColumn("sentiment_score", sentimentToValue(col("result")))
    scoredDf.printSchema()

    val finalDf = scoredDf.select("created_at", "coordinates", "hashtags", "text", "sentiment_score")

    finalDf
      .writeStream
      .format("mongodb")
      .option("checkpointLocation", "tmp/checkPointTweetsMod")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

}
