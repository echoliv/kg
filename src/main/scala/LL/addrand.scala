
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.jblas.{Solve, DoubleMatrix}

import scala.collection.immutable.Map

object addrand {
  def main(args: Array[String]) {
    /*
    val conf = new SparkConf()
      .setAppName("Fresh_song_v1_1_" + args(0))
      .set("spark.rdd.compress", "true")

    val sc = new SparkContext(conf)
    */
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
    val conf =new SparkConf().setAppName("k-means").setMaster("local").set("spark.driver.allowMultipleContexts","true");
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    add(args, sc)
    sc.stop()
  }

  def add(strings: Array[String], sc: SparkContext): Unit ={
    val old = "1=a1,a2,a3,a4,a5,a6,a7,a8,a9,a10"
    val rand = "1=b1,b2,b3"

    val oldlist = sc.textFile(old).map{ x =>

    }

  }

}
