package data

import org.apache.spark.sql.SparkSession

object SpecialUserFeatureExport {

  def select_feature(line: (String, (String, Option[String]))): (String, String) = {
    val features = line._2._1.split("=")
    val f1 = features.slice(0, 76)    // 76
    val f2 = features.slice(149, 282)  //133
    val special_f_str = new Array[Int](51).mkString("=")
    val f_special = line._2._2.getOrElse(special_f_str)
    (line._1,  "%s=%s=%s".format(f1.mkString("="), f2.mkString("="), f_special))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("fm_user_feature").
      config("spark.rdd.compress", "true").
      config("spark.driver.maxResultSize", "0").
      config("spark.default.parallelism", "1000").
      config("spark.executor.extraJavaOptions", "-Xloggc:/data1/app/spark_executor_gc_logs/executorGC.log -verbose:gc -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100m -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewRatio=4 -XX:NewRatio=4  -XX:CMSFullGCsBeforeCompaction=5 -XX:+UseCMSCompactAtFullCollection").
      enableHiveSupport().
      getOrCreate()

    import spark.implicits._

    val dt = args(0)

    val sql_all =
      """
         select b.user_id, concat(songcnt_reduce,'=',user_features)
         from
         (
            select userid, split(user_features,'=')[0] as songcnt_reduce
            from analyse.lhy_fm_ctr_user_all_features_v3
            where dt='%s' and substr(userid,-1,1)='9' and cast(substr(userid,-2,1) as int)%%5=4
         ) a
         inner join
         (
            select user_id, user_features
            from analyse.gyb_all_user_predict_features_temp_mf_action
            where dt='%s' and substr(user_id,-1,1)='9' and cast(substr(user_id,-2,1) as int)%%5=4
         ) b
         on a.userid=b.user_id
      """.format(dt, dt)
    println(sql_all)
    val sql_special =
      """
        select user_id, user_features
        from analyse.gyb_special_user_features_special_mf_predict
        where dt='%s'
      """.format(dt)
    val all_feature = spark.sql(sql_all).rdd.map(x => (x(0).toString, x(1).toString))
    val special_feature = spark.sql(sql_special).rdd.map(x => (x(0).toString, x(1).toString))
    all_feature.leftOuterJoin(special_feature)
      .map(select_feature)
      .toDF("user_id", "features")
      .createOrReplaceTempView("tmp")

    val sql_insert =
      """
        insert overwrite table analyse.gyb_special_user_features_predict_day partition (dt='%s')
          select user_id, features
          from tmp
      """.format(dt)
    spark.sql(sql_insert)
  }
}
