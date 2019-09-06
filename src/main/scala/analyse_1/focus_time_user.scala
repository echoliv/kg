package analyse_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.{DoubleMatrix, Solve}

import scala.collection.immutable.Map

object focus_time_user {
  def main(args: Array[String]): Unit = {
/*
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
    val conf = new SparkConf().setAppName("k-means").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
*/
    val conf = new SparkConf()
    .setAppName(args(0))
    .set("spark.rdd.compress", "true")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //读取训练数据

    val user_play =args(1)
    val input = args(2)
    val gapfilter=args(3).toInt
//    val lst1 = new ListBuffer[String]

    var user_play_time = sc.textFile(user_play).map { line =>
      var userid = line.split('|')(0)
      var timeseq = line.split('|')(1).split(',')

      var seqlength = timeseq.length
      var temp = timeseq(0).toDouble
      var temph = 0.0
      var tempq = 0.0
      var len = 0.0
      var str = ""
      val lst1 = new ListBuffer[String]
      //      var userlist:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
      for (i <- 1 to seqlength - 1) {
        temph = timeseq(i).toDouble
        tempq = timeseq(i - 1).toDouble
        if (i == seqlength - 1 && temp - temph < gapfilter) {
          len = temph - temp
          str =temph.toString + ':' + len.toString
          lst1 += str
        }
        if (temph - tempq >= gapfilter) {
          len = tempq- temp
          str = tempq.toString + ':' + len.toString
          lst1 += str
          temp = temph
        }
      }
      (userid+'|'+lst1.mkString(","))
    }


    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(input), true)
    } catch {
      case _: Throwable => {}
    }
    user_play_time.saveAsTextFile(input)
  }
}