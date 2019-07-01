/**
  * Created by zhongzhao on 2016/11/18.
  * This program is the training part of LatentCF_v10_3, and all the modifications are for LatentCF_v10_3_increment.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.jblas.{DoubleMatrix, _}

import scala.collection.immutable.Map

object LatentCF_v10_3_train {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("LatentCF_v10_3_train_" + args(0))
      .set("spark.rdd.compress", "true")
      .set("spark.yarn.executor.memoryOverhead", "5000")
      .set("spark.yarn.driver.memoryOverhead", "6000")
      .set("spark.yarn.am.memoryOverhead", "5000")
      .set("spark.yarn.am.memory", "1000")
      .set("spark.executor.cores", "3")
      .set("spark.driver.maxResultSize", "0")
      .set("spark.default.parallelism", "3200")
      .set("spark.executor.extraJavaOptions", "-Xloggc:/data1/app/spark_executor_gc_logs/executorGC.log -verbose:gc -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100m -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewRatio=4 -XX:NewRatio=4  -XX:CMSFullGCsBeforeCompaction=5 -XX:+UseCMSCompactAtFullCollection")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // data sources and parameters
    val trainDataPath = args(1)
    val songIDFPath = args(2)
    val productFeaturePath = args(3)
    val parameters = args(4)

    // setting the parameters
    val alpha = parameters.split('_')(0).toDouble
    val lambda = parameters.split('_')(1).toDouble
    val rank = parameters.split('_')(2).toInt
    val numIteration = parameters.split('_')(3).toInt

    // read the training data from hive table: mid - songid - playdur
    val trainRating = sc.textFile(trainDataPath).map(line => line.split('|')).map { line =>
      try { (line(0), line(1), line(2).toDouble) }
    }

    // read the IDF from hive table: songid - idf
    val song_idf = sc.textFile(songIDFPath).map{ x => x.split('|')}.map{ x => (x(0), x(1).toDouble)}.collect.toMap
    val B_songidf = sc.broadcast(song_idf)

    // change user-item interations into this format: mid - songid_list - playdur_list
    val userRows = trainRating.groupBy(_._1).map { row =>
      val (indices, values) = row._2.map(e => (e._2, e._3)).unzip
      (row._1, indices.toArray, values.toArray)
    }
    userRows.persist(StorageLevel.MEMORY_AND_DISK)

    // change user-item interations into this format: songid - mid_list - playdur_list
    val productRows = trainRating.groupBy(_._2).map { row =>
      val (indices, values) = row._2.map(e => (e._1, e._3)).unzip
      (row._1, indices.toArray, values.toArray)
    }
    productRows.persist(StorageLevel.MEMORY_AND_DISK)

    // define the pre-computed variables
    var P1 = new DoubleMatrix(rank, rank)
    val P3 = DoubleMatrix.eye(rank).mmul(lambda)
    val B_P3 = sc.broadcast(P3)

    // initialization of the product features
    var productFeatures = productRows.map { x =>
      val tmpFeature = DoubleMatrix.rand(rank).toArray
      (x._1, tmpFeature)
    }
    val tep = DoubleMatrix.rand(3)
    var pfmap = productFeatures.collect.toMap

    // initialization of the user features
    var userFeatures = userRows.map { x =>
      val tmpFeature = DoubleMatrix.rand(rank).toArray
      (x._1, tmpFeature)
    }
    var ufmap = userFeatures.collect.toMap

    // performing the ALS iterations
    for (i <- 1 to numIteration) {

      // updata user features
      val allSongid = pfmap.keys.toArray
      val songNum = allSongid.length
      P1 = new DoubleMatrix(rank, rank)
      for (j <- 0 to songNum - 1) {
        val tmpFactor = new DoubleMatrix(pfmap(allSongid(j)))
        P1 = P1.add(tmpFactor.mmul(tmpFactor.transpose))
      }
      val B_P1 = sc.broadcast(P1)
      val B_pfmap = sc.broadcast(pfmap)
      pfmap = null
      userFeatures = userRows.map { line =>
        val mid = line._1
        val songid = line._2
        val playdur = line._3
        val uf = updateUserFeatures(songid, playdur, alpha, rank, B_P1.value, B_P3.value, B_pfmap.value, B_songidf.value)
        (mid, uf.toArray)
      }
      ufmap = userFeatures.collect.toMap

      // update product features
      val mids = ufmap.keys.toArray
      val userNum = mids.length
      P1 = new DoubleMatrix(rank, rank)
      for (j <- 0 to userNum - 1) {
        val tmpFactor = new DoubleMatrix(ufmap(mids(j)))
        P1 = P1.add(tmpFactor.mmul(tmpFactor.transpose))
      }
      val B_PP1 = sc.broadcast(P1)
      val B_ufmap = sc.broadcast(ufmap)
      ufmap = null
      productFeatures = productRows.map { line =>
        val songid = line._1
        val mid = line._2
        val playdur = line._3
        val idf = B_songidf.value(songid)
        val pf = updateItemFeatures(mid, playdur, idf, alpha, rank, B_PP1.value, B_P3.value, B_ufmap.value)
        (songid, pf.toArray)
      }
      pfmap = productFeatures.collect.toMap

    } /* loop over the ALS iterations*/

    // save productFeatures for increment computation.
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {hdfs.delete(new org.apache.hadoop.fs.Path(productFeaturePath), true)}// catch {case _: Throwable => {}}
    productFeatures.map{ x => x._1 + "=" + x._2.mkString("|")}.saveAsTextFile(productFeaturePath)

  }



  def updateUserFeatures(songid: Array[String], playdur: Array[Double], alpha: Double, rank: Int, P1: DoubleMatrix, P3: DoubleMatrix, pfmap: scala.collection.immutable.Map[String,Array[Double]], song_idf: scala.collection.immutable.Map[String,Double]): DoubleMatrix = {
    val songNum = songid.length
    var uf = new DoubleMatrix(rank)
    if(songNum > 1000){
      var P2 = new DoubleMatrix(rank, rank)
      var B = new DoubleMatrix(rank)
      for(i <- 0 to songNum - 1){
        val tmpFeature = pfmap(songid(i))
        val tmpy = new DoubleMatrix(tmpFeature)
        val c = 1 + alpha * Math.log10(1 + playdur(i)/0.01) * song_idf(songid(i))
        P2 = P2.add(tmpy.mmul(c-1).mmul(tmpy.transpose))
        B = B.add(tmpy.mmul(c))
      }
      val A = P1.add(P2).add(P3.mmul(songNum))
      uf = Solve.solve(A, B)
    }else{
      val tmpFeature = new Array[Array[Double]](songNum)
      val rating = new DoubleMatrix(songNum)
      for (i <- 0 to songNum - 1) {
        tmpFeature(i) = pfmap(songid(i))
        rating.put(i, Math.log10(1 + playdur(i)/0.01) * song_idf(songid(i)))
      }
      val tmpY = new DoubleMatrix(tmpFeature)
      val tmpC = rating.mmul(alpha).add(1.0)
      val tmpD = DoubleMatrix.diag(tmpC)
      val tmpI = DoubleMatrix.eye(songNum)
      val P2 = tmpY.transpose.mmul(tmpD.sub(tmpI)).mmul(tmpY)
      val A = P1.add(P2).add(P3.mmul(songNum))
      val B = tmpY.transpose.mmul(tmpC)
      uf = Solve.solve(A, B)
    }
    uf
  }


  def updateItemFeatures(mid: Array[String], playdur: Array[Double], idf: Double, alpha: Double, rank: Int, P1: DoubleMatrix, P3: DoubleMatrix, ufmap: Map[String, Array[Double]]): DoubleMatrix = {
    val userNum = mid.length
    var pf = new DoubleMatrix(rank)
    if(userNum > 1000){
      var P2 = new DoubleMatrix(rank, rank)
      var B = new DoubleMatrix(rank)
      for(i <- 0 to userNum - 1){
        val tmpFeature = ufmap(mid(i))
        val tmpy = new DoubleMatrix(tmpFeature)
        val c = 1 + alpha * Math.log10(1 + playdur(i)/0.01) * idf
        P2 = P2.add(tmpy.mmul(c-1).mmul(tmpy.transpose))
        B = B.add(tmpy.mmul(c))
      }
      val A = P1.add(P2).add(P3.mmul(userNum))
      pf = Solve.solve(A, B)
    }else{
      val tmpFeature = new Array[Array[Double]](userNum)
      val rating = new DoubleMatrix(userNum)
      for (i <- 0 to userNum - 1) {
        tmpFeature(i) = ufmap(mid(i))
        rating.put(i, Math.log10(1 + playdur(i)/0.01) * idf)
      }
      val tmpX = new DoubleMatrix(tmpFeature)
      val tmpC = rating.mmul(alpha).add(1.0)
      val tmpD = DoubleMatrix.diag(tmpC)
      val tmpI = DoubleMatrix.eye(userNum)
      val P2 = tmpX.transpose.mmul(tmpD.sub(tmpI)).mmul(tmpX)
      val A = P1.add(P2).add(P3.mmul(userNum))
      val B = tmpX.transpose.mmul(tmpC)
      pf = Solve.solve(A, B)
    }
    pf
  }
}
