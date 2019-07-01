package similar_specialid

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, _}
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.recognition.impl.StopRecognition
//import org.ansj.library.UserDefineLibrary

object test_special {
  def main(args: Array[String]): Unit = {
    /*
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
    */
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
    val conf = new SparkConf().setAppName("k-means").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
    val sc = new SparkContext(conf)
    //    import spark.implicits._
    val t1 = ToAnalysis.parse("就让我带你进入我的世界,让你懂我的一切")
    val FilterModifWord = new StopRecognition()

    for (i<-0 to t1.size()-1){
      val s = FilterModifWord.filter(t1.get(i))
      if(!FilterModifWord.filter(t1.get(i))){
        println(t1.get(i))
      }

    }


  }

}
