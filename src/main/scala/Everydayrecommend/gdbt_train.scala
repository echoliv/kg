//线上
import java.io.{BufferedWriter, FileWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object train_gbdt_v3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Everyday_recommendation_" + args(0))
      .set("spark.rdd.compress", "true")
      .set("spark.yarn.executor.memoryOverhead", "4000")
      .set("spark.yarn.driver.memoryOverhead", "6000")
      .set("spark.executor.cores", "3")
      .set("spark.driver.maxResultSize", "0")
      .set("spark.default.parallelism", "3200")
      .set("spark.executor.extraJavaOptions", "-Xloggc:/data1/app/spark_executor_gc_logs/executorGC.log -verbose:gc -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100m -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewRatio=4 -XX:NewRatio=4  -XX:CMSFullGCsBeforeCompaction=5 -XX:+UseCMSCompactAtFullCollection")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val datasets = args(1)
    val outputFile = args(2)
    val gbdt_parameters = args(3)
    val aucPath = args(4)

    println(args.mkString("\n"))
    val numIters = gbdt_parameters.split('_')(0).toInt
    val maxDepth = gbdt_parameters.split('_')(1).toInt
    val learningRate = gbdt_parameters.split('_')(2).toDouble
    val trainData = sc.textFile(datasets).repartition(300).map { x =>
      val label_feature = x.split('|')
      val label = label_feature(0).toDouble
      val feature = label_feature(1).split('=').map { s => s.toDouble }
      LabeledPoint(label, Vectors.dense(feature))
    }


    val Array(trainingData, testData) = trainData.randomSplit(Array(0.8, 0.2), seed = 12345)


    trainingData.persist(StorageLevel.MEMORY_AND_DISK)
    trainingData.take(1)

    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = numIters
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = maxDepth
    boostingStrategy.treeStrategy.maxBins = 32
    boostingStrategy.treeStrategy.subsamplingRate = 0.5
    boostingStrategy.learningRate = learningRate
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)


    val scoreAndLabels = testData.map {
      x =>
        val pred = model.predict(x.features)
        (pred, x.label)
    }
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)

    val areaUnderROC = metrics.areaUnderROC

    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)

    writeFile(aucPath, date + " | areaUnderROC = " + areaUnderROC + "\n")

    model.save(sc, outputFile)
  }


  def writeFile(filePath: String, str: String): Unit = {
    val bw = new BufferedWriter(new FileWriter(filePath, true))
    bw.append(str)
    bw.close()
  }
}
