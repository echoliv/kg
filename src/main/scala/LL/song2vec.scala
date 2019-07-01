package kugou.songSimilar

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.{DoubleMatrix, Solve}

import scala.collection.immutable.Map

object songToVec {
  def main(args: Array[String]){

  /*
    val conf = new SparkConf()
      .setAppName("songToVec")
      .set("spark.rdd.compress", "true")
      .set("spark.storage.memoryFraction", "0.2")
      .set("spark.executor.cores", "2")
      .set("spark.default.parallelism", "100")
      .set("spark.shuffle.safetyFraction", "0.5")
      .set("spark.executor.extraJavaOptions","-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:PermSize=256M -XX:MaxPermSize=256M")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)
    */
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
    val conf =new SparkConf().setAppName("k-means").setMaster("local").set("spark.driver.allowMultipleContexts","true");
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val seqFile = "file\\song2vec_1"
    val save_file = "/user/hive/warehouse/analyse.db/t_lxx_song2vec_result/"


    val songSeq = sc.textFile(seqFile).map { line =>
      val k = line.split('!')(0)
      val result=line.split('!')(1).split(',').toSeq
      (k,result)
    }
    songSeq.foreach(println)

    val songSeqDF = sqlCtx.createDataFrame(songSeq).toDF("key","songSeq")
    val word2Vec = new Word2Vec().setInputCol("songSeq").setOutputCol("result").setVectorSize(200).setMinCount(200).setNumPartitions(20).setMaxIter(10)
    val model = word2Vec.fit(songSeqDF)
    model.save("file\\song2vecmodel")
    /*
    val vcoabV = model.getVectors.map{x =>
      (x.getAs[String]("word"),x.getAs[DenseVector]("vector"))
    }.map{ line =>
      val k = line._1
      val v = line._2.toArray
      (k,v)
    }
    val vcoabVMap =   vcoabV.collect.toMap
    val B_vcoabV = sc.broadcast(vcoabVMap)
    val specialN = vcoabVMap.size
    val cosin = vcoabV.flatMap{ line =>
      val k = line._1
      val v = line._2
      val tmpV = new DoubleMatrix(v)
      val squareLV = tmpV.norm2()
      val tmpCosin = new Array[(String,Double)](specialN)
      var t = 0
      val tempK = B_vcoabV.value.keys.toArray

      for (ii <- 0 to tempK.length - 1){
        val k1 = tempK(ii)
        val v1 = B_vcoabV.value(k1)
        val tmpV1 = new DoubleMatrix(v1)
        val squareLV1 = tmpV1.norm2()
        val cosinV = tmpV1.dot(tmpV)/(squareLV*squareLV1)
        tmpCosin(ii) = (k1, cosinV)
      }
      tmpCosin.map{ line =>
        (k,line._1,line._2)
      }
      val similarSort = tmpCosin.toSeq.sortWith(_._2 > _._2)
      val resultLength = 300
      val result =  new Array[(String,Double)](resultLength)
      for (i <- 0 to resultLength - 1){
        result(i) = (similarSort(i)._1,similarSort(i)._2)
      }
      result.map{ line =>
        (k,line._1,line._2)
      }
    }
    println(cosin)

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(save_file), true)
    } catch {
      case _: Throwable => {}
    }
*/
  }
}
