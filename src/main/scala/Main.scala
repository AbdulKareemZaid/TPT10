import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    // Create a SparkConf object
    val conf = new SparkConf()
      .setAppName("Twitter Stream Processing")
      .setMaster("local[*]")
    // Create a SparkSession using the SparkConf object
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // Call the run method from StreamProcessing object
    KafkaConnection.run(spark)
  }
}
