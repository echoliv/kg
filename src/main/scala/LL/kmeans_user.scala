import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.mllib.clustering.KMeans.fastSquaredDistance
import org.apache.spark.mllib.clustering.{KMeans, VectorWithNorm}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.util.MLUtils

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet






object kmeans_user {
  def main(args: Array[String]) {


    kmeans("1.txt","1.txt")
  }

  def  kmeans(trianurl:String,forecasturl:String)={
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
    val conf =new SparkConf().setAppName("k-means").setMaster("local").set("spark.driver.allowMultipleContexts","true");
    val sc = new SparkContext(conf)
    val data = sc.textFile(trianurl)
    val userid = List("")
//    val parsedData = data.map(s => Vectors.dense(s.split('\t')(1).split('=').map(_.toDouble))).cache()
    val parsedData = data.map{line =>
      val d1 = line.split(' ')(0)
      val d2 = line.split(' ')
      val d3 = d2(d2.length-1).split('=')
      Vectors.dense(d3.map(_.toDouble))
    }
    parsedData.foreach(println)
    val numClusters = 5  //将目标数据分成几类
    val numIterations = 30 //迭代的次数

    val kss = "2_5_8_9".split('_')

    kss.foreach(clusternum => {
      val model = KMeans.train(parsedData, clusternum.toInt, numIterations)
      val ssd = model.computeCost(parsedData)
      println(" when k=" + clusternum + " -> "+ ssd)
    })

    //将参数，和训练数据传入，形成模型
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    val s=clusters.getClass()
    print(s)
    clusters.clusterCenters.foreach(println)
    val centers = clusters.clusterCenters
    val length = centers.length
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

//    var usergroup: List[List[String]] = new List()
    //输入需要分类处理的数据
    val forecastdata = sc.textFile(forecasturl)

    val forecastdata1=forecastdata.map{line=>
      val d1 = line.split(' ')(0)
      val d2 = line.split(' ')
      val d3 = d2(d2.length-1).split('=')
//      val r1 = clusters.predict(Vectors.dense(d3.map(_.toDouble)))
//      val group = r1.toInt
//      println(d1 + ' '+ group)
      var temp = Vectors.dense(d3.map(_.toDouble))
      var user_info = ""
      for(i<- 0 until length) {
        val tempcenter = centers(i)
        var sum = 0.0;
        for(j<- 0 until temp.size){
          sum = sum + (temp(j) - tempcenter(j))*(temp(j) - tempcenter(j))
        }
        user_info = user_info +'|'+ i.toString +':'+sum.toString
      }
      (d1,user_info.substring(1,user_info.length()-1))
    }

    val ks:Array[Int] = Array(2,4,6,8,10)
    ks.foreach(clusternum => {
      val model = KMeans.train(parsedData, clusternum, numIterations)
      val ssd = model.computeCost(parsedData)
      println(" when k=" + clusternum + " -> "+ ssd)
    })

    forecastdata1.foreach(println)
    val result = clusters.predict(parsedData)
    //打印分类结果
    result.foreach(println)

  }




}
