//线上
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{Vector => LibVector, Vectors => LibVectors}
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, functions => F}
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

object predict_gbdt {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Everyday_recommendation_predict_sort_prob_" + args(0))
      .set("spark.rdd.compress", "true")
      .set("spark.yarn.executor.memoryOverhead", "6000")
      .set("spark.yarn.driver.memoryOverhead", "6000")
      .set("spark.executor.cores", "2")
      .set("spark.default.parallelism", "3200")
      .set("spark.executor.extraJavaOptions", "-Xloggc:/data1/app/spark_executor_gc_logs/executorGC.log -verbose:gc -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100m -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewRatio=4 -XX:NewRatio=4  -XX:CMSFullGCsBeforeCompaction=5 -XX:+UseCMSCompactAtFullCollection")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._
    val user_songList_path = args(1)
    val model_path = args(2)
    val userInfoPath = args(3)
    val songInfoPath = args(4)
    val userSongNums = args(5).toInt
    val group = args(6) //"8-1" "8" "all"
    val dt = args(7)
    val output = args(8)
    println(args.mkString("|"))
    val user_songList = sc.textFile(user_songList_path).map{ line =>
      val user_songList_split = line.split('=')
      val userid = user_songList_split(0)
      val lenUser = userid.length
      var userid_select:String = "no_select"
      if(group == "all"){
        userid_select = userid
      }
      else if(lenUser >= 3 && group.split('-').length >= 2){
        try {
          val v1 = group.split('-')(0).toInt
          val v2 = group.split('-')(1).toInt
          if(userid.substring(lenUser-1).toInt==v1 && userid.substring(lenUser-3,lenUser-1).toInt%5==v2 )
          {userid_select = userid}
        } catch {case _: Throwable => {}}
      }
      else if(group.split('-').length == 1){
        try {
          val v = group.split('-')(0).toInt
          if(userid.substring(lenUser-1).toInt==v )
          {userid_select = userid}
        } catch {case _: Throwable => {}}
      }
      val songList = user_songList_split(1).split( ',')
      (userid_select,songList)
    }.toDF("userid","songList")
    val user_song = user_songList.withColumn("songid",F.explode(F.col("songList"))).filter(F.col("userid")!=="no_select")
    user_song.show
    val song_features = sc.textFile(songInfoPath).map{line =>
      val songid = line.split('|')(0)
      val song_features = line.split('|')(1).split('=').map(_.toDouble)
      (songid,song_features)
    }.toDF("songid","song_features")
    val user_features = sc.textFile(userInfoPath).map{line =>
      val userid = line.split('|')(0)
      val user_features = line.split('|')(1).split('=').map(_.toDouble)
      (userid,user_features)
    }.toDF("userid","user_features")
    val concatUDF = F.udf{
      (user_features:Seq[Double],song_features:Seq[Double]) =>
        val features = user_features ++ song_features
        Vectors.dense(features.toArray)
    }
    val user_song_features = user_song.join(song_features,Seq("songid")).join(user_features,Seq("userid")).
      withColumn("features",concatUDF($"user_features",$"song_features")).
      select("userid","songid","features")
    val mergeUDF = F.udf((strings:Seq[String])=>strings.mkString("|"))
    val no_user_features = user_song.join(user_features,Seq("userid"),"left_outer")
    val res1 = no_user_features.filter(F.col("user_features").isNull).select("userid","songid")
    //.groupBy("userid").agg(F.collect_set(F.col("songid")).alias("songid_set")).
    //withColumn("songids",mergeUDF(F.col("songid_set")))
    val model = GradientBoostedTreesModel.load(sc,model_path)

    // Get class probability dataframe => rdd
    val testData = user_song_features.rdd.map{
      case Row(userid:String,songid:String,features:Vector) =>
        (userid,songid,LibVectors.dense(features.toArray))
    }
    //    val treePredictions = testData.map { row => model.trees.map(_.predict(row._3)) }
    //    val treePredictionsVector = treePredictions.map(array => LibVectors.dense(array))
    //    val treePredictionsMatrix = new RowMatrix(treePredictionsVector)
    //    val learningRate = model.treeWeights
    //    val learningRateMatrix = Matrices.dense(learningRate.size, 1, learningRate)
    //    val weightedTreePredictions = treePredictionsMatrix.multiply(learningRateMatrix)
    //    val classProb = weightedTreePredictions.rows.flatMap(_.toArray).map(x => 1 / (1 + Math.exp(-1 * x)))
    val res2Proba = testData.map{row =>
      val userid = row._1
      val songid = row._2
      val features = row._3
      val proba = sigmoid(score(features,model))
      (userid,songid,proba)
    }.toDF("userid","songid","prob")
    //val extractP = F.udf((probability:Vector) => probability.toArray(1))
    //val res2ProbaP = res2Proba.withColumn("prob",extractP(F.col("probability")))
    val res2Rank = res2Proba.withColumn("pRank",F.row_number().over(Window.partitionBy("userid").
      orderBy(F.desc("prob"))))
    val res2 = res2Rank.filter(F.col("pRank")<=userSongNums).select("userid","songid")
    //.groupBy("userid").agg(F.collect_set(F.col("songid")).alias("songid_set")).
    //withColumn("songids",mergeUDF(F.col("songid_set"))).select("userid","songid_set")
    val res = res1.union(res2)
    res.createOrReplaceTempView("tmp")
    //res.write.mode("overwrite").saveAsTable("%s".format(output))
    val sql_string ="""
                    insert overwrite table %s partition(dt='%s')
                    select userid, songid
                    from tmp
                    """.format(output,dt)
    sqlContext.sql(sql_string)
  }
  def score(features: LibVector,gbdt: GradientBoostedTreesModel) : Double = {
    val treePredictions = gbdt.trees.map(_.predict(features))
    val v1 = new DoubleMatrix(treePredictions).transpose
    val v2 = new DoubleMatrix(gbdt.treeWeights)
    //blas.ddot(gbdt.numTrees, treePredictions, 1, gbdt.treeWeights, 1)
    v1.mmul(v2).toArray()(0)
  }
  def sigmoid(v : Double) : Double = {
    1/(1+Math.exp(-v))
  }
}
