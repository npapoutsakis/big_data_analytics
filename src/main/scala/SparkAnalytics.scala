import org.apache.spark.sql.SparkSession

object SparkAnalytics extends App {

  val spark = SparkSession.builder.master("local[*]")
    .appName("Spark Analytics Project")
    .getOrCreate()


  spark.stop()
}
