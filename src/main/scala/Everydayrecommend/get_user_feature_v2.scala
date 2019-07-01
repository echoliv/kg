//线上每日推荐用户特征计算
package EverydayRecommendation

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix
import Array._


object get_user_feature_v2 {

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
    val spark = SparkSession.builder().appName("evereyday_recommendation_user_feature").
      config("spark.rdd.compress", "true").
      config("spark.yarn.executor.memoryOverhead", "4000").
      config("spark.yarn.driver.memoryOverhead", "6000").
      config("spark.executor.cores", "3").
      config("spark.driver.maxResultSize", "2000").
      config("spark.default.parallelism", "1000").
      config("spark.executor.extraJavaOptions", "-Xloggc:/data1/app/spark_executor_gc_logs/executorGC.log -verbose:gc -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100m -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewRatio=4 -XX:NewRatio=4  -XX:CMSFullGCsBeforeCompaction=5 -XX:+UseCMSCompactAtFullCollection").
      enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    println(args.mkString("\n"))

    val sliceFinal = 73
    val per = 0.8
    val dt = args(0)
    val songFeaturePath = args(1)
    val favorHistoryPath = args(2)
    val user_feature_table = args(3)


    val favorHistory = sc.textFile(favorHistoryPath).
      map {
        x =>
          val userId = x.split('|')(0)
          val songId = x.split('|')(1)
          val score = x.split('|')(2)
          //val score = x.split('\t')(3)
          (userId, songId + ":" + score)
      }.reduceByKey((x, y) => x + "," + y)


    favorHistory.take(3).foreach(println)
    favorHistory.persist()

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
    val userFeature = favorHistory.map {
      row =>
        val userId = row._1
        val songAndScoreList = row._2
          .split(',')
          .map(e => (e.split(':')(0), e.split(':')(1).toDouble))
          .filter(e => songFeature_b.value.contains(e._1))

        val scoreSum = songAndScoreList.map(e => e._2).sum
        var feature = DoubleMatrix.zeros(songFeatureLength)
        for ((songId, score) <- songAndScoreList) {
          val songFeature = new DoubleMatrix(songFeature_b.value.getOrElse(songId, DoubleMatrix.zeros(songFeatureLength).toArray))
          feature = if (scoreSum > 0) songFeature.mmul(score * 1.0 / scoreSum).add(feature) else DoubleMatrix.zeros(songFeatureLength)
        }
        //73
        val feature_d = feature.getRange(0, sliceFinal).toArray
        //132-73=59
        val feature_other = feature.getRange(sliceFinal, songFeatureLength).toArray
        //73
        val feature_c = feature_d.map(e => if (e >= per) 1.0 else 0.0)
        (userId, arr_format(concat(feature_d, feature_c, feature_other)))
    }
    userFeature.toDF("user_id", "user_features").createOrReplaceTempView("tmp")
    val sql_insert =
      """
        insert overwrite table %s partition (dt='%s')
          select user_id, user_features
          from tmp
      """.format(user_feature_table, dt)
    spark.sql(sql_insert)
  }
}

