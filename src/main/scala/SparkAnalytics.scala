import org.apache.spark.sql.SparkSession

object SparkAnalytics extends App {

  val doc_c_data = "hdfs://localhost:9000/reuters/rcv1-v2.topics.qrels"
  val doc_c = "hdfs://localhost:9000/reuters/doc_c_results"

  val doc_t_data = "hdfs://localhost:9000/reuters/lyrl2004_vectors_train.dat"
//    "hdfs://localhost:9000/reuters/lyrl2004_vectors_test_pt0.dat",
//    "hdfs://localhost:9000/reuters/lyrl2004_vectors_test_pt2.dat",
//    "hdfs://localhost:9000/reuters/lyrl2004_vectors_test_pt3.dat",
//    "hdfs://localhost:9000/reuters/lyrl2004_vectors_test_pt1.dat",

  val doc_t = "hdfs://localhost:9000/reuters/doc_t_results"

  val spark = SparkSession.builder.master("local[*]")
    .appName("Spark Analytics Project")
    .getOrCreate()

  //***************************************************************************************

  // DOC(C) Structure - (category_id, [doc1, doc2, doc3, ...]+)
  val rcv1_v2 = spark.sparkContext.textFile(doc_c_data)
    .map(line => (line.split(" ")(0), line.split(" ")(1)))    //parsing data (category_id, document_id)
    .groupByKey()                                             //grouping by key category_id (category_id, [doc1, doc2, doc3, ...]+)
    .map(x => (x._1, x._2.toSet))                             //2nd arg to set

  // |DOC(C)|
  val rcv1_v2_result = rcv1_v2.mapValues(_.size)
  println(rcv1_v2_result.collect().mkString(", "))

  //***************************************************************************************

  // DOC(T) Structure - (document_id, [term1, term2, term3, ...]+)
  val terms_per_doc = spark.sparkContext.textFile(doc_t_data)
    .map(line => {
      val terms = line.split(" ")
        .tail
        .map(x => (x.split(":")(0)))
        .filterNot(_.isEmpty)
        .toSeq

      (line.split(" ")(0), terms)
    })

  // DOC(T) Structure - (term, [doc1, doc2, doc3, ...]+)
  val docs_per_term = terms_per_doc.flatMap({
    case (doc_id, terms) => terms.map(t => (t, doc_id))
  })
    .distinct()
    .groupByKey()
    .map(x => (x._1, x._2.toSet))

  // |DOC(T)|
  val doc_t_result = docs_per_term.mapValues(_.size)
  println(docs_per_term.collect().mkString(","))

  //***************************************************************************************



  // Terminating Spark Session
  spark.stop()
}
