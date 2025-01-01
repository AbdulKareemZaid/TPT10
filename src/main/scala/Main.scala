import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Twitter Stream Processing")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    Consumer.run(spark)

  }
}
