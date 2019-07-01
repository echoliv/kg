package LL


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

object word2vec {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("LatentCF_v10_6_increment_" + args(0))
      .set("spark.rdd.compress", "true")

    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

  }
}