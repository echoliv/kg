import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.log4j.Logger
import org.apache.log4j.Level

object RandomForst {
  def main(args: Array[String]) {
    val inputpath = args(0)
    val outputpath = args(1)
    val conf = new SparkConf()
      .setAppName(args(0))
      .set("spark.rdd.compress", "true")
      .set("spark.executor.cores", "5")
      .set("spark.default.parallelism", "1280")
      .set("spark.yarn.executor.memoryOverhead", "3000")
      .set("spark.yarn.driver.memoryOverhead", "3000")
      .set("spark.driver.maxResultSize", "0")
      .set("spark.cleaner.referenceTracking.blocking","true")
      .set("spark.cleaner.referenceTracking.blocking.shuffle","true")
      .set("spark.akka.timeout","600s")
      .set("spark.network.timeout","600s")
      .set("spark.task.maxFailures","8")
      .set("spark.yarn.max.executor.failures","100")
      //      .set("spark.files.fetchTimeout","600s")
      //      .set("spark.executor.heartbeatInterval","60s")
      //      .set("spark.shuffle.io.maxRetries","5")
      //      .set("spark.shuffle.file.buffer","128k")
      //      .set("spark.shuffle.io.retryWait","30s")
      //      .set("spark.dynamicAllocation.cachedExecutorIdleTimeout","30s")
      //      .set("spark.locality.wait","0")
      //      .set("spark.dynamicAllocation.executorIdleTimeout","  30s")
      //      .set("spark.dynamicAllocation.schedulerBacklogTimeout","3s")
      //      .set("spark.dynamicAllocation.cachedExecutorIdleTimeout","30s")
      //      .set("spark.dynamicAllocation.minExecutors","20")
      //      .set("spark.dynamicAllocation.maxExecutors","120")
      //      .set("spark.dynamicAllocation.enabled","true")
      .set("spark.executor.extraJavaOptions", "-Xloggc:/data1/app/spark_executor_gc_logs/executorGC.log -verbose:gc -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100m -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewRatio=4 -XX:NewRatio=4  -XX:CMSFullGCsBeforeCompaction=5 -XX:+UseCMSCompactAtFullCollection")

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val sc = new SparkContext(conf)

    // 加载数据
    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, inputpath)
//    val data1 = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    // 将数据随机分配为两份
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))
    // 随机森林训练参数设置
    //分类数
    val numClasses = 2
    // categoricalFeaturesInfo 为空，意味着所有的特征为连续型变量
    val categoricalFeaturesInfo = Map[Int, Int]()
    //树的个数
    val numTrees = 100
    //特征子集采样策略，auto 表示算法自主选取
    val featureSubsetStrategy = "auto"
    //纯度计算
    val impurity = "gini"
    //树的最大层次
    val maxDepth = 50
    //特征最大装箱数
    val maxBins = 32
    //训练随机森林分类器，trainClassifier 返回的是 RandomForestModel 对象
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // 测试数据评价训练好的分类器并计算错误率
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification forest model:\n" + model.toDebugString)

    // 将训练后的随机森林模型持久化
    model.save(sc, "myModelPath")
    //加载随机森林模型到内存
    val sameModel = RandomForestModel.load(sc, "myModelPath")
    sc.stop()
  }
}
