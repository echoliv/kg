
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

object similar_topN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("special_similarity_cut_" + args(0))
      .set("spark.rdd.compress", "true")
      .set("spark.yarn.executor.memoryOverhead", "5000")
      .set("spark.yarn.driver.memoryOverhead", "6000")
      .set("spark.executor.cores", "2")
      .set("spark.driver.maxResultSize", "0")
      .set("spark.default.parallelism", "3200")
      .set("spark.executor.extraJavaOptions", "-Xloggc:/data1/app/spark_executor_gc_logs/executorGC.log -verbose:gc -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100m -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewRatio=4 -XX:NewRatio=4  -XX:CMSFullGCsBeforeCompaction=5 -XX:+UseCMSCompactAtFullCollection")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)


    val norm_features = args(1)
    val norm_featuresnew = args(2)
    val savepath = args(3)
    val sim_song_num = args(4).toInt


    val data = sc.textFile(norm_features).map { line =>
      try {
        val s: Array[String] = line.split('=')
        val id = s(0)
        //val norm: Double = s(1).toDouble
        val feature: Array[Double] = s(2).split('|').map(_.toDouble)
        val f_m = new DoubleMatrix(feature)
        val norm= f_m.norm2()
        (id, (norm, feature))
      }
    }.cache()

    val datanew = sc.textFile(norm_featuresnew).map { line =>
      try {
        val s: Array[String] = line.split('=')
        val id = s(0)
        //val norm: Double = s(1).toDouble
        val feature: Array[Double] = s(2).split('|').map(_.toDouble)
        val f_m = new DoubleMatrix(feature)
        val norm= f_m.norm2()
        (id, (norm, feature))
      }
    }.cache()




    for (c <- 0 to 9) {
      val data_b = data.filter(_._1.endsWith(c.toString)).cache()

      val (ids, nf) = data_b.collect.unzip

      val norms = new DoubleMatrix(nf.toArray.map(_._1))
      val features = new DoubleMatrix(nf.toArray.map(_._2))

      val B_ids = sc.broadcast(ids.toArray)
      val B_norms = sc.broadcast(norms)
      val B_features = sc.broadcast(features)
      val songNum = data_b.count().toInt




      val result: RDD[(String, String, Double)] = datanew.flatMap { x =>
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
