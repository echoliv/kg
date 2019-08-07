//计算歌单特征
//歌曲特征平移
package similar_specialid

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix
import Array._


object special_feature {

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
    val spark = SparkSession.builder().appName("special_feature").
      config("spark.rdd.compress", "true").
      config("spark.executor.extraJavaOptions", "-Xloggc:/data1/app/spark_executor_gc_logs/executorGC.log -verbose:gc -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100m -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewRatio=4 -XX:NewRatio=4  -XX:CMSFullGCsBeforeCompaction=5 -XX:+UseCMSCompactAtFullCollection").
      enableHiveSupport().
      getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    println(args.mkString("\n"))

    val dt = args(0)
    val songFeaturePath = args(1)
    val songList = args(2)
    val special_feature_table = args(3)


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

    // 获取歌曲特征的size
    songFeature.take(10).foreach(println)
    val songFeatureLength = songFeature.take(1).values.toList.head.length
    println("=========================================================")
    println(songFeatureLength)

    // 广播歌曲特征
    val songFeature_b = sc.broadcast(songFeature)

    // 计算用户的特征
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
          val songFeature = new DoubleMatrix(songFeature_b.value.getOrElse(songId, DoubleMatrix.zeros(songFeatureLength).toArray))
          feature = songFeature.add(feature)
        }

        feature = feature.mmul(1.0/countsong)
        val f = arr_format(feature.toArray)
        (specialId, f)
    }
    specialFeature.toDF("special_id", "specialid_features").createOrReplaceTempView("tmp")
    val sql_insert =
      """
        insert overwrite table %s partition (dt='%s')
          select special_id, special_features
          from tmp
      """.format(special_feature_table, dt)
    spark.sql(sql_insert)
  }
}

