object predictlist {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("als")
      .set("spark.rdd.compress", "true")
      .set("spark.executor.cores", "2")
      .set("spark.default.parallelism", "1280")
      .set("spark.yarn.executor.memoryOverhead", "5000")
      .set("spark.yarn.driver.memoryOverhead", "6000")
      .set("spark.driver.maxResultSize", "0")
      .set("spark.executor.extraJavaOptions", "-Xloggc:/data1/app/spark_executor_gc_logs/executorGC.log -verbose:gc -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100m -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewRatio=4 -XX:NewRatio=4  -XX:CMSFullGCsBeforeCompaction=5 -XX:+UseCMSCompactAtFullCollection")

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val input_preuser_train_file = ""
    val input_sond_feature_file = ""
    val goodsong_file = ""
    val badsong_file = ""
    val output_predict_list =""

  }

}
