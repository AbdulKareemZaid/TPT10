import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    System.setProperty("log4j.configuration", "file:src/main/resources/log4j.properties")

    val conf = new SparkConf()
      .setAppName("Twitter Stream Processing")
      .setMaster("local[*]")
      .set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // start consumer & processing data
    StreamProcessingConsumerApp.start(spark)

    spark.streams.awaitAnyTermination()
  }
}
