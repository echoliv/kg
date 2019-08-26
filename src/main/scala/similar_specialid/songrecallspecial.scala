/**
  * Created by echoliv on 2019/8/19.
  */

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

object song_recall_special {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(args(0)+"_recall_special" )
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


//    val norm_features = args(1)

    val song_features = args(1)
    val special_features =args(2)
    val savepath = args(3)
    val sim_song_num = args(4).toInt

    val song_f = sc.textFile(song_features).map { line =>
      val s =line.split('|')
      val id =s(0)
      val feature = s(1).split('=').map(_.toDouble)
      val f_m = new DoubleMatrix(feature)
      val norm = f_m.norm2()
      (id,(norm,feature))
    }
    song_f.take(3).map(x=> (x._1+'|'+x._2._1+'|'+x._2._2.mkString(","))).foreach(println)


    val special_f = sc.textFile(special_features).map { line =>
      val s =line.split('|')
      val id =s(0)
      val feature = s(1).split('=').map(_.toDouble)
      val f_m = new DoubleMatrix(feature)
      val norm = f_m.norm2()
      (id,(norm,feature))
    }.cache()
    special_f.take(3).map(x=> (x._1+'|'+x._2._1+'|'+x._2._2.mkString(","))).foreach(println)

    val (ids,nf) = special_f.collect.unzip
    val norms = new DoubleMatrix(nf.map(_._1))
    val feature = new DoubleMatrix(nf.map(_._2))


    val result_s = song_f.flatMap{x =>
      val songid = x._1
      val songnorm = x._2._1
      val songfeature = new DoubleMatrix(x._2._2)
      val cossims = feature.mmul(songfeature).mmul(1/songnorm).divColumnVector(norms)

      val sort_index = cossims.sortingPermutation().reverse
      val recom_result_nofilter = sort_index.slice(0,sim_song_num).map{e=> (songid,ids(e),cossims.get(e))}
      recom_result_nofilter
    }

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(savepath), true)
    } catch {
      case _: Throwable => {}
    }
    result_s.map(x =>  (x._1+'|'+x._2+'|'+x._3)).saveAsTextFile(savepath)
  }
}
