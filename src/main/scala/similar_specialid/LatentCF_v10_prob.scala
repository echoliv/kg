package similar_specialid

/**
  * Created by zhongzhao on 2016/10/8.
  * This program is to compute the probability of preference of user to item. It uses random forest to predict the probability.
  */
//猜歌单排序（songid-specialid）
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.Array._

object LatentCF_v10_prob {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("LatentCF_v10_prob_" + args(0))
      .set("spark.rdd.compress", "true")
      .set("spark.yarn.executor.memoryOverhead", "5000")
      .set("spark.yarn.driver.memoryOverhead", "5000")
      .set("spark.executor.cores", "3")
      .set("spark.default.parallelism", "640")
      .set("spark.executor.extraJavaOptions", "-Xloggc:/data1/app/spark_executor_gc_logs/executorGC.log -verbose:gc -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100m -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewRatio=4 -XX:NewRatio=4  -XX:CMSFullGCsBeforeCompaction=5 -XX:+UseCMSCompactAtFullCollection")
    //.set("spark.driver.maxResultSize", "0")

    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //val mf_trainDataPath = args(1)
    val songInfoPath = args(1)
    val userInfoPath = args(2)
    val incrementDataPath = args(3)
    val rf_trainDataPath = args(4)
    val savePath0 = args(5)
    val savePath1 = args(6)
    val rf_parameter = args(7)

    //val trainSongList = sc.textFile(mf_trainDataPath).map{x=>x.split('|')}.map{x=>x(1)}.distinct.map{x => (x, 1)}

    val rfModel = train_RF_model(rf_trainDataPath, rf_parameter, sc)

    val songInfoData = sc.textFile(songInfoPath).map{ x => x.split('|')}.map{ x =>
      val songid = x(0)
      val pf = x(1).split('=').map{s=>s.toDouble}
      (songid, pf)
    } //.join(trainSongList).map{x=>(x._1, x._2._1)}
    val songInfoMap = songInfoData.collect.toMap

    val userInfoData = sc.textFile(userInfoPath).map{ x => x.split('|')}.map{ x =>
      val mid = x(0)
      val uf = x(1).split('=').map{s=>s.toDouble}
      (mid, uf)
    }

    val userRows = sc.textFile(incrementDataPath).map{ x => x.split('=') }.map{ x =>
      val mid = x(0)
      val songlist = x(1).split(',')
      (mid, songlist)
    }.leftOuterJoin(userInfoData).map{ x => (x._1, x._2._1, x._2._2) }.cache()

    val testData: DataFrame = userRows.filter{ x=>x._3!=None}.flatMap{ x =>
      val mid = x._1
      val songlist = x._2
      val n = songlist.length
      val mid_songid_features = new Array[(String, String, Vector)](n)
      val uf = x._3.toList(0)
      for(i <- 0 to n - 1){
        if(songInfoMap.contains(songlist(i))){
          val pf = songInfoMap(songlist(i))
          val combinedFeature = concat(uf, pf)
          mid_songid_features(i) = (mid, songlist(i), Vectors.dense(combinedFeature))
        }
      }
      mid_songid_features.filter{x=>x!=null}
    }.toDF("mid", "songid", "features")

    val predictions: RDD[(String, String, Double)] = rfModel.transform(testData).select("mid", "songid", "probability").map { x =>
      val mid = x.get(0).toString
      val songid = x.get(1).toString
      val prob = x.get(2).toString.split('[')(1).split(']')(0).split(',')(1).toDouble
      (mid, songid, prob)
    }.rdd

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    try {hdfs.delete(new org.apache.hadoop.fs.Path(savePath1), true)} catch {case _: Throwable => {}}
    predictions.map(x => x._1 + "|" + x._2 + "|" + x._3).saveAsTextFile(savePath1)

    try {hdfs.delete(new org.apache.hadoop.fs.Path(savePath0), true)} catch {case _: Throwable => {}}
    userRows.filter{x=>x._3==None}.map{x=>x._1+"="+x._2.mkString(",")}.saveAsTextFile(savePath0)

  }

  def train_RF_model(trainDataPath: String, parameters: String, sc:SparkContext): org.apache.spark.ml.PipelineModel = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val numTree = parameters.split('_')(0).toInt
    val maxDepth = parameters.split('_')(1).toInt
    val impurity = parameters.split('_')(2)

    val trainingSamples = sc.textFile(trainDataPath).map{ x =>
      val label_feature = x.split('=')
      val label = label_feature(0).toDouble
      val feature = label_feature(1).split('|').map{s=>s.toDouble}
      (label, Vectors.dense(feature))
    }.toDF("label","features")

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(trainingSamples)

    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(16).fit(trainingSamples)

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(numTree)
      .setMaxDepth(maxDepth)
      .setImpurity(impurity)

    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf))
    val model = pipeline.fit(trainingSamples)
    model
  }

}
