//      INF424 - Functional Programming, Analytics & Applications 2024
//               Application Scenario 2: Vessel Analytics
//      Team Members:
//                  Nikolaos Papoutsakis 2019030206
//                  Sokratis Siganos     2019030097

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

object VesselAnalytics extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Vessel Trajectory Analytics")
    .getOrCreate()

  FileSystem.setDefaultUri(spark.sparkContext.hadoopConfiguration,"hdfs://localhost:9000")
  val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)


  // data
  val input_data = "hdfs://localhost:9000/nmea_aegean/nmea_aegean.logs"
  val query_output_1 = "hdfs://localhost:9000/nmea_aegean/query_output_1"
  val query_output_2 = "hdfs://localhost:9000/nmea_aegean/query_output_2"
  val query_output_3 = "hdfs://localhost:9000/nmea_aegean/query_output_3"
  val query_output_4 = "hdfs://localhost:9000/nmea_aegean/query_output_4"
  val query_output_5 = "hdfs://localhost:9000/nmea_aegean/query_output_5"

  // Dataframe
  val vessel_df = spark.read.option("header", "true").csv(input_data)
  val timestamp_to_date = vessel_df.withColumn("timestamp", to_date(col("timestamp")))


  //**********************************Query 1***************************************
  hdfs.delete(new Path(query_output_1), true)
  val query_1_and_2_setup = timestamp_to_date.select("timestamp", "station", "mmsi")

  // show on driver screen
  val query_1 = query_1_and_2_setup
    .distinct()
    .groupBy("timestamp", "station")
    .count()
    .orderBy("timestamp")

  query_1.show()

  // save to file
  query_1.rdd.map(x => s"Date: ${x(0)} | Station: ${x(1)} | Vessel Trackings: ${x(2)}").saveAsTextFile(query_output_1)



  //**********************************Query 2***************************************
  hdfs.delete(new Path(query_output_2), true)
  val query_2 = query_1_and_2_setup
    .groupBy("mmsi")
    .count()
    .orderBy(desc("count"))
    .limit(1)

  // show on driver screen
  query_2.show()

  // save to file
  query_2.rdd.map(x => s"Vessel ID (mmsi): ${x(0)} with ${x(1)} tracked positions").saveAsTextFile(query_output_2)



  //**********************************Query 3***************************************
  hdfs.delete(new Path(query_output_3), true)

  // Vessels at both stations each day
  val vessels_visible_on_both_stations = timestamp_to_date
    .filter(col("station").isin("8006", "10003"))
    .groupBy("timestamp", "mmsi")
    .agg(count("station") as "station_count")
    .filter(col("station_count") === 2)

  val query_3 = timestamp_to_date
    .join(vessels_visible_on_both_stations, Seq("timestamp", "mmsi"))
    .filter(col("station").isin("8006", "10003"))
    .select("speedoverground")
    .agg(avg("speedoverground") as "Average SOG")

  // show on driver screen
  query_3.show()

  // save to file
  query_3.rdd.map(x => s"Average SOG: ${x(0)}").saveAsTextFile(query_output_3)



  //**********************************Query 4***************************************
  hdfs.delete(new Path(query_output_4), true)
  val query_4 = timestamp_to_date
    .withColumn("abs_dif", abs(col("heading") - col("courseoverground")))
    .groupBy("station")
    .agg(avg("abs_dif"))

  // show on driver screen
  query_4.show()

  // save to file
  query_4.rdd.map(x => s"Station ${x(0)} | Average Difference ${x(1)}").saveAsTextFile(query_output_4)



  //**********************************Query 5***************************************
  hdfs.delete(new Path(query_output_5), true)
  val query_5 = timestamp_to_date
    .groupBy("status")
    .count()
    .orderBy(desc("count"))
    .limit(3)
  // show on driver screen
  query_5.show()

  // save to file
  query_5.rdd
    .zipWithIndex()
    .map({
      case (x, index) =>
        s"${index + 1}. Status ${x(0)} with ${x(1)} appearances"
    })
    .saveAsTextFile(query_output_5)



  //**********************************End***************************************
  // Terminating Spark Session
  spark.stop()
}
