//      INF424 - Functional Programming, Analytics & Applications 2024
//                       Application Scenario 1
//      Team Members:
//                  Nikolaos Papoutsakis 2019030206
//                  Sokratis Siganos     2019030097

import org.apache.spark.sql.SparkSession

object SparkAnalytics extends App {

  val category_file = "hdfs://localhost:9000/reuters/rcv1-v2.topics.qrels"
  val terms_file = "hdfs://localhost:9000/reuters/lyrl2004_vectors_*.dat"
  val stems_file = "hdfs://localhost:9000/reuters/stem.termid.idf.map.txt"

  val categories_out = "hdfs://localhost:9000/reuters/categories_out"
  val terms_out = "hdfs://localhost:9000/reuters/terms_out"
  val output = "hdfs://localhost:9000/reuters/JaccardIndex"

  //  val checkpoint_dir = "hdfs://localhost:9000/reuters/checkpoint"

  val spark = SparkSession.builder.master("local[*]")
    .appName("Spark Analytics Project")
    .config("spark.driver.memory", "4g")
    .config("spark.num.executors", "3")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "2g")
    .getOrCreate()

  //  spark.sparkContext.setCheckpointDir(checkpoint_dir)

  //***************************************************************************************
  // STEMS
  val rdd_stems_file = spark.sparkContext.textFile(stems_file)
    .map(x => {
      val line = x.split(" ")
      (line(1), line(0)) // termid stem
    })

  // CATEGORIES
  val rdd_categories_file = spark.sparkContext.textFile(category_file)
    .map(x => {
      val line = x.split(" ")
      (line(1), line(0)) //parsing data (docid, catid)
    })

  val rdd_terms_file = spark.sparkContext.textFile(terms_file)
    .map(x => {
      val line = x.split("\\s+")
      val terms = line
        .tail
        .map(y => y.split(":")(0))
      (line.head, terms)
    })
    .flatMap({
      case (doc_id, terms) => terms.map(t => (doc_id, t))
    }) //  docid, termid

  val rdd_categories_docs = rdd_categories_file
    .map(x => (x._2, 1)) // cat, 1
    .reduceByKey(_+_) // cat, DOC(C)

  // TODO: join stems here or later?
  val rdd_terms_docs = rdd_terms_file
    .map(x => (x._2, 1))
    .reduceByKey(_+_) // term, DOC(T)

  val rdd_cogrouped = rdd_categories_file.cogroup(rdd_terms_file) // docid , (Seq(cats), Seq(terms))

  val rdd_intersection = rdd_cogrouped
    .flatMap({
      case(docid, (categories, terms)) => {
        terms.flatMap { t =>
          categories.map{ c =>
            ((c,t),1)
          }
        }
      }
    })
    .reduceByKey(_+_) // ((cat, term), intersection)
    .map(x => (x._1._1, (x._1._2, x._2))) //

  val rdd_joined = rdd_intersection.join(rdd_categories_docs)
      .map(x => {
        val (category, ((term, intersection), doc_c)) = x
        (term, (category, doc_c, intersection))
      })
    .join(rdd_terms_docs)
    .map(x => {
      val (term, ((category, doc_c, intersection), doc_t)) = x
      (term, (category, s"${intersection.toFloat / (doc_c + doc_t - intersection)}"))
    })
    .join(rdd_stems_file)
    .map(x => {
      val (term, ((category, ji), stem)) = x
      s"$category;$stem;$ji"
    })

  rdd_joined.saveAsTextFile(output)


  spark.stop()
}