import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}


object kmeans_group {
  def main(args: Array[String]) {

    val numc = args(1)
    val numit = args(2)
    val inputraintpath = args(3)
    val forecastpath = args(4)
    val outputpath = args(5)
    val clusterCenterspath = args(6)
    val array=args(7)

    val conf = new SparkConf()
      .setAppName(args(0))
      .set("spark.rdd.compress", "true")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    val data = sc.textFile(inputraintpath)

    val parsedData = data.map(s => Vectors.dense(s.split('|')(1).split('=').map(_.toDouble))).cache()

    val numClusters = numc.toInt  //将目标数据分成几类
    val numIterations = numit.toInt //迭代的次数

    //将参数，和训练数据传入，形成模型
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    val s=clusters.getClass()
    //打印中心点
    val centers = clusters.clusterCenters
    val length = centers.length
    clusters.clusterCenters.foreach(println)
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
//    println("Within Set Sum of Squared Errors = " + WSSSE)

    //输入需要分类处理的数据
    val forecastdata = sc.textFile(forecastpath)


    val forecastdata1=forecastdata.map{line=>


      val d1 = line.split('|')(0)
      val d2 = line.split('|')(1).split('=')

      var temp = Vectors.dense(d2.map(_.toDouble))
      var user_info = ""
      for(i<- 0 until length) {
        val tempcenter = centers(i)
        var sum = 0.0;
        for(j<- 0 until temp.size){
          sum = sum + (temp(j) - tempcenter(j))*(temp(j) - tempcenter(j))
        }
        user_info = user_info +';'+ i.toString +':'+sum.toString
      }
      (d1,user_info.substring(1,user_info.length()-1))
    }
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(outputpath), true)
    } catch {
      case _: Throwable => {}
    }
    forecastdata1.map{x=>
       x._1 + '=' + x._2
    }.saveAsTextFile(outputpath)

    val ks = array.split('_')
    ks.foreach(clusternum => {
      val model = KMeans.train(parsedData, clusternum.toInt, numIterations)
      val ssd = model.computeCost(parsedData)
      println(" when k=" + clusternum + " -> "+ ssd)
    })


    //    val result = clusters.predict(parsedData)
    //打印分类结果
//    result.foreach(println)
  }
}
