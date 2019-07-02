package similar_specialid

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by bearlin on 2015/11/19.
 */
object specialSimilarityTrain {
  def main(args: Array[String]){
    val conf = new SparkConf()
      .setAppName("specialSimilarityTrain")
      .set("spark.rdd.compress", "true")
      .set("spark.storage.memoryFraction", "0.2")
      .set("spark.executor.cores", "2")
      .set("spark.default.parallelism", "320")
      .set("spark.shuffle.safetyFraction", "0.5")
      .set("spark.executor.extraJavaOptions","-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:PermSize=256M -XX:MaxPermSize=256M")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

   //读取训练数据
   val sample_file = args(0)
   val pairs_file = args(1)
    //val sample_file = "/user/hive/warehouse/analyse.db/recommendation_specialsim_sample/*/*"
    //val pairs_file = "/user/hive/warehouse/analyse.db/recommendation_specialsim_pairs/dt=2016-10-08/*"
    //读取训练参数
    val resultFile = args(2)
    //val tagCandidateFile = args(3)

    //val resultFile = "/user/hive/warehouse/analyse.db/recommendSpecial/"
    //val tagCandidateFile = "/user/hive/warehouse/analyse.db/recommendation_specialsim_candidate/"

    //对训练数据进行处理，并进行下采样，再转化成dataframe
  /*  val tagCandidate = sc.textFile(tagCandidateFile).map{line =>
      val s = line.split('|')
      (s(0).toInt,s(1).split(',').toList)
    }
   */



    val trainingSamples = sc.textFile(sample_file).map{ line =>
      val s = line.split('|')
      val colle = s(2).split(',').map(x => x).toList
      var label = 0.0
      var mark = 1.0
      val r = scala.util.Random
      if (colle.contains(s(3))){
        label = 1.0
      } else {
        label = 0.0
        if(r.nextFloat>0.1){
          mark = 0.0
        }
      }
      (label,s.slice(4, s.size).map(_.toDouble),mark,s(1),s(3))
    }.filter(x => x._3==1.0).map(x=>(x._1,Vectors.dense(x._2))).toDF("label","features")


    val predict_pairs = sc.textFile(pairs_file).map{ line =>
      val s = line.split('|')
      (s(0) + '_' + s(1),s.slice(2, s.size).map(_.toDouble))
    }.map(x=>(x._1,Vectors.dense(x._2))).toDF("pairs","features")
    //对类标和特征进行编码，变成pipleline可读取的格式
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(trainingSamples)

    //多次训练并测试
/*    var score_label = sc.parallelize(new Array[(Double, Double)](0))
    for(nfold <- 1 to 1){
      val Array(trainingData, testData) = trainingSamples.randomSplit(Array(0.7, 0.3))
      val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(100).setMaxDepth(10).setImpurity("entropy")
      val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf))
      val model = pipeline.fit(trainingData)
      val predictions = model.transform(testData)
      val score_label_tmp = predictions.select("probability","label").map{x=>
        val score = x.get(0).toString.split('[')(1).split(']')(0).split(',')(1).toDouble
        val label = x.getDouble(1)
        (score, label)
      }
      score_label = score_label.union(score_label_tmp)
    }
    //求auc
    val metrics = new BinaryClassificationMetrics(score_label,100)
    val auc = metrics.areaUnderROC()
    val roc = metrics.roc
    roc.collect.foreach(x=>println(x._1.toString+'|'+x._2.toString))
*/
    //构建随机森林并进行训练
    val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("features").setNumTrees(100).setMaxDepth(15).setImpurity("entropy")
    val pipeline = new Pipeline().setStages(Array(labelIndexer, rf))
    val model = pipeline.fit(trainingSamples)
    //对候选对进行预测，并使用标签集进行过滤
    val predictions = model.transform(predict_pairs).select("pairs","probability").map{line =>
      val score = line.get(1).toString.split('[')(1).split(']')(0).split(',')(1).toDouble
      (line.get(0).toString.split('_')(0),line.get(0).toString.split('_')(1),score)
    }.map(x => (x._1,(x._2,x._3))).rdd.groupByKey().flatMap{x =>
      val resultK = 18
      val specialid = x._1
      val recommendSpecial = x._2.toSeq.sortWith(_._2 > _._2)
      var resultLength = resultK
      val recommendLength = recommendSpecial.length
      if (recommendSpecial.length > resultK){
        resultLength = resultK
      } else {
        resultLength = recommendSpecial.length
      }
      val result =  new Array[(String,Double)](resultLength)

      for (i <- 0 to resultLength - 1){
        result(i) = (recommendSpecial(i)._1,recommendSpecial(i)._2)
      }
      result.map{ line =>
        (specialid,line._1,line._2)
      }
    }

    //输出
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(resultFile), true) } catch { case _ : Throwable => { } }

    predictions.map( x => x._1.toString + "|" + x._2.toString + "|" + x._3.toString).repartition(5).saveAsTextFile(resultFile, classOf[GzipCodec])

    //预测并输出


  }
}
