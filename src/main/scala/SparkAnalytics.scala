//      INF424 - Functional Programming, Analytics & Applications 2024
//                       Application Scenario 1
//      Team Members:
//                  Nikolaos Papoutsakis 2019030206
//                  Swkratis Siganos     2019030087

import org.apache.spark.sql.SparkSession

object SparkAnalytics extends App {

  val doc_c_data = "hdfs://localhost:9000/reuters/rcv1-v2.topics.qrels"
  val doc_t_data = Seq("hdfs://localhost:9000/reuters/lyrl2004_vectors_train.dat",
    "hdfs://localhost:9000/reuters/lyrl2004_vectors_test_pt0.dat",
    "hdfs://localhost:9000/reuters/lyrl2004_vectors_test_pt2.dat",
    "hdfs://localhost:9000/reuters/lyrl2004_vectors_test_pt3.dat",
    "hdfs://localhost:9000/reuters/lyrl2004_vectors_test_pt1.dat")
  val stems_data = "hdfs://localhost:9000/reuters/stem.termid.idf.map.txt"

  val output = "hdfs://localhost:9000/reuters/output"

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
  val doc_c_result = rcv1_v2.mapValues(_.size)

  //***************************************************************************************
  // DOC(T) Structure - (document_id, [term1, term2, term3, ...]+)
  val terms_per_doc = spark.sparkContext.textFile(doc_t_data.mkString(","))
    .map(y => {
      val line = y.split("\\s+")
      val terms = line
        .tail
        .map(x => x.split(":")(0))
      (line.head, terms)
    })

  // DOC(T) Structure - (term_id, [doc1, doc2, doc3, ...]+)
  val docs_per_term = terms_per_doc.flatMap({
    case (doc_id, terms) => terms.map(t => (t, doc_id))
  })
    .groupByKey()
    .map(x => (x._1, x._2.toSet))

  // |DOC(T)|- (term_id, number_of_documents)+
  val doc_t_result = docs_per_term.mapValues(_.size)

  //***************************************************************************************
  // DOC(T)∩DOC(C) - Structure
  // Broadcast the DOC(C) RDD so it can be used in the map operation efficiently
  val doc_c_broadcast = spark.sparkContext.broadcast(rcv1_v2.collectAsMap())

  // For each term in DOC(T), find the intersection with each category in DOC(C)
  val intersections = docs_per_term.flatMap {
      case (term, doc_t_set) =>
        doc_c_broadcast.value.map {
          case (category, doc_c_set) =>
            (term, category, doc_t_set.intersect(doc_c_set))
        }
    }

  //***************************************************************************************

  // Stem_ID Structure - (term_id -> stem_id)
  val stems = spark.sparkContext.textFile(stems_data)
    .map(line => {
      (line.split(" ")(1), line.split(" ")(0))
    })
    .collectAsMap()


  //***************************************************************************************
  // DOC(C) Structure - (category_id, [doc1, doc2, doc3, ...]+)+
  // DOC(T) Structure - (term_id, [doc1, doc2, doc3, ...]+)+
  // DOC(T)∩DOC(C) Structure - (category_id_C, term_id_T, category_id_C^term_id_T)+
  // JaccardIndex Structure (jac_idx(c, t) = category_id_C(intersection)term_id_T/category_id_C(union)term_id_T

  val doc_t_map = doc_t_result.collectAsMap()
  val doc_c_map = doc_c_result.collectAsMap()

  val jaccard_index = intersections.map({
    case (term_id, category_id, intersection) => {
      // need |DOC(T)|, |DOC(C)| and |DOC(T)∩DOC(C)|
      val numerator = intersection.size
      val denominator = doc_t_map(term_id) + doc_c_map(category_id) + intersection.size
      val stem_id = stems(term_id)
      (s"$category_id;$stem_id;${numerator.toFloat/denominator}")
    }
  })

  //save to file hdfs
  jaccard_index.saveAsTextFile(output)
  
  // Terminating Spark Session
  spark.stop()
}
