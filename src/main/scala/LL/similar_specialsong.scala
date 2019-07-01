/**
  * Created by sophialiang on 2017/5/25.
  */

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

object similar_specialsong {

  def main(args: Array[String]) {
    /*
    //    val save_user_file = args(6)
    val conf = new SparkConf()
      .setAppName(args(0))
      .set("spark.rdd.compress", "true")
    */
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
    val conf =new SparkConf().setAppName("k-means").setMaster("local").set("spark.driver.allowMultipleContexts","true");
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val sc = new SparkContext(conf)
    computesimilar(args,sc)
  }

  def computesimilar(args: Array[String],sc:SparkContext): Unit = {

//    val norm_features = args(1)
//    val savepath = args(2)
//    val sim_song_num = args(4).toInt
    val norm_features="file\\similar_song_feature.txt"
    val savepath = "file\\similar_song_5"
    val sim_song_num = 5


    val data = sc.textFile(norm_features).map { line =>
      val s: Array[String] = line.split('=')
      val id = s(0)
      val feature: Array[Double] = s(1).split('|').map(_.toDouble)
      val f_m = new DoubleMatrix(feature)
      val norm= f_m.norm2()
      (id, (norm, feature))
      }
    data.foreach(println)



    for (c <- 0 to 9) {
      val data_b = data.filter(_._1.endsWith(c.toString)).cache()

      val (ids, nf) = data_b.collect.unzip

      val norms = new DoubleMatrix(nf.toArray.map(_._1))
      val features = new DoubleMatrix(nf.toArray.map(_._2))

      val B_ids = sc.broadcast(ids.toArray)
      val B_norms = sc.broadcast(norms)
      val B_features = sc.broadcast(features)
      val songNum = data_b.count().toInt




      val result: RDD[(String, String, Double)] = data.flatMap { x =>
        val ids = B_ids.value
        val id1 = x._1
        val norm1: Double = x._2._1
        val feature1 = new DoubleMatrix(x._2._2)
        val cossims = features.mmul(feature1).mmul(1 / norm1).divColumnVector(norms)


        val sort_index: Array[Int] = cossims.sortingPermutation().reverse
        val recom_result_nofilter = sort_index.slice(0, sim_song_num).map { e => (id1, ids(e), cossims.get(e)) }

        var singerCount: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.Map()
        var (i: Int, j: Int) = (0, 0)
        recom_result_nofilter
      }

      val nofilter_savepath_part = savepath.concat("_").concat(c.toString)
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

      try {hdfs.delete(new org.apache.hadoop.fs.Path(nofilter_savepath_part), true)}// catch {case _: Throwable => {}}
      result.map{ x => x._1 + "|" + x._2 + "|" + x._3}.saveAsTextFile(nofilter_savepath_part)

    }


  }
}
