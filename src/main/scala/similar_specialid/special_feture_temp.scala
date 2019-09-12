//计算歌单特征
//歌曲特征平移
package similar_specialid

import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix
import Array._


object special_feature_temp {

  def arr_format(arr: Array[Double]): String = {
    arr.map(x=>
    {
      val v = x.formatted("%.5f")
      if(v.endsWith(".00000"))
        x.toInt.toString
      else if (v.endsWith("000"))
        x.formatted("%.2f")
      else
        v
    }).mkString("=")
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
    val conf = new SparkConf().setAppName("k-means").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //读取训练数据

    val songFeaturePath = "file\\songfeature"
    val songList = "file\\specialsong"
    val special_feature_table = "file\\special_feature"



    val favorHistory = sc.textFile(songList).
      map {
        x =>
          val userId = x.split('|')(0)
          val songId = x.split('|')(1)
          //val score = x.split('\t')(3)
          (userId, songId )
      }.reduceByKey((x, y) => x + "," + y)


    favorHistory.take(3).foreach(println)

    // songId|songFeature
    val songFeature = sc.textFile(songFeaturePath).map {
      x =>
        (x.split('|')(0), x.split('|')(1).split('=').map(e => e.toDouble))
    }.collectAsMap()

    val temp = songFeature.take(1).map(x=> (x._1+'|'+x._2.mkString(",")) )
    temp.take(1).foreach(println)
    // 获取歌曲特征的size
    songFeature.take(10).foreach(println)
    val songFeatureLength = songFeature.take(2).values.toList.head.length
    println("=========================================================")
    println(songFeatureLength)

    // 广播歌曲特征
    val songFeature_b = sc.broadcast(songFeature)

    // 计算歌单的特征
    val specialFeature = favorHistory.map {
      row =>
        val specialId = row._1
        val songAndScoreList = row._2
          .split(',')
          .map(e => e.toString)
          .filter(e => songFeature_b.value.contains(e))

        val countsong = songAndScoreList.length

        var feature = DoubleMatrix.zeros(songFeatureLength)
        for ((songId) <- songAndScoreList) {
          //          val songId =songAndScoreList(i)
          val songFeature = new DoubleMatrix(songFeature_b.value.getOrElse(songId, DoubleMatrix.zeros(songFeatureLength).toArray))
          feature = songFeature.add(feature)
        }

        feature = feature.mmul(1.0/countsong)
        val f = arr_format(feature.toArray)
        (specialId, f)
    }


    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(special_feature_table), true)
    } catch {
      case _: Throwable => {}
    }

    specialFeature.map(x=> x._1)
    specialFeature.map(x => (x._1+'|'+x._2)).saveAsTextFile(special_feature_table)

  }
}

