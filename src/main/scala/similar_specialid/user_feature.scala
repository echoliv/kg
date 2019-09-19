package similar_specialid

import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

object UserFeature {
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
    val spark = SparkSession.builder().appName("special_user_feature").
      config("spark.rdd.compress", "true").
      config("spark.yarn.executor.memoryOverhead", "4000").
      config("spark.yarn.driver.memoryOverhead", "6000").
      config("spark.executor.cores", "3").
      config("spark.driver.maxResultSize", "0").
      config("spark.default.parallelism", "1000").
      config("spark.executor.extraJavaOptions", "-Xloggc:/data1/app/spark_executor_gc_logs/executorGC.log -verbose:gc -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100m -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewRatio=4 -XX:NewRatio=4  -XX:CMSFullGCsBeforeCompaction=5 -XX:+UseCMSCompactAtFullCollection").
      enableHiveSupport().
      getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    println(args.mkString("\n"))

    val dt = args(0)
    val songFeaturePath = args(1)
    val user_song_path = args(2)
    val special_feature_table = args(3)
    val feature_num = args(4).toInt

    // songId|songFeature
    val songFeature = sc.textFile(songFeaturePath).map {
      x =>
        (x.split('|')(0), new DoubleMatrix(x.split('|')(1).split('=').take(feature_num).map(e => e.toDouble)))
    }.cache()

    val favor_history = sc.textFile(user_song_path).map {
      x =>
        // userid, songcnt_reduce, songid, playdur
        (x.split('|')(0), x.split('|')(3).toDouble, x.split('|')(1), x.split('|')(2).toDouble)
    }.cache()

    val user_playdur_sum = favor_history.map(x=>(x._3, (x._1, x._4))).join(songFeature.map(x=>(x._1, 1)))
      .map(x=>(x._2._1._1, x._2._1._2)).reduceByKey(_+_)

    val favor_history_norm = favor_history.map(x=>(x._1, (x._2, x._3, x._4)))
      .join(user_playdur_sum)
      // (userid, ((songcnt_reduct, songid, playdur), playdur_sum))
      .map{
      x =>
        val userid = x._1
        val songid = x._2._1._2
        val songcnt_reduce = x._2._1._1
        val playdur = x._2._1._3
        val playdur_sum = x._2._2
        (songid, (songcnt_reduce, userid, playdur/playdur_sum))
    }
    favor_history.unpersist()




    // (songid, ((songcnt_reduce, userid, playdur), features) )
    val favorHistoryAndFeature = favor_history_norm.join(songFeature)

    val userFeature =
      favorHistoryAndFeature
        .map {
          x =>
            val userid = x._2._1._2
            val songFeature = x._2._2
            val songcnt_reduce = x._2._1._1
            val playdur = x._2._1._3
            ((userid, songcnt_reduce), songFeature.mmul(playdur))
        }.reduceByKey((x, y) => x.add(y))
        .map {
          x =>
            val userid = x._1._1
            val songcnt_reduce = x._1._2
            val feature = x._2.toArray
            (userid, "%s=%s".format(songcnt_reduce, arr_format(feature)))

        }


    userFeature.toDF("user_id", "user_features").createOrReplaceTempView("tmp")
    val sql_insert =
      """
        insert overwrite table %s partition (dt='%s')
          select user_id, user_features
          from tmp
      """.format(special_feature_table, dt)
    spark.sql(sql_insert)
  }
}
