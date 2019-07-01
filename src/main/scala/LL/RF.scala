package RF

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{VectorIndexer, StringIndexer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame
import scala.Array._
import org.apache.log4j.Logger
import org.apache.log4j.Level



class modelSelection(dataPath: String, sc: SparkContext) {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  val training: DataFrame = sc.textFile(dataPath).map{ x => x.split('|')}.map{ x =>
    val label = x(0).toDouble
    val features: Array[Double] = x(1).split('=').map(_.toDouble)
    (label, Vectors.dense(features))
  }.toDF("label", "features")

  def run(parameters: String,nFold: Int): Array[(ParamMap, Double)] = {
    val parametersArray: Array[String] = parameters.split('=')
    val numTrees: Array[Int] = parametersArray(0).split(',').map(_.toInt)
    val maxDepth: Array[Int] = parametersArray(1).split(',').map(_.toInt)
    val impurity = parametersArray(2).split(',')


    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel")

    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(2)

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")


    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf))


    val paramGrid: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(rf.numTrees, numTrees)
      .addGrid(rf.maxDepth,maxDepth)
      .addGrid(rf.impurity, impurity)
      .build()

    val evaluator: BinaryClassificationEvaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderROC")
      .setRawPredictionCol("probability")
      .setLabelCol("indexedLabel")



    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(nFold)

    val cvModel = cv.fit(training)

    // Print the average metrics per ParamGrid entry
    val avgMetricsParamGrid: Array[Double] = cvModel.avgMetrics
    // Combine with paramGrid to see how they affect the overall metrics
    val combined: Array[(ParamMap, Double)] = paramGrid.zip(avgMetricsParamGrid)
    combined
  }



}
