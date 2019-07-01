package kugou

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Created by bearlin on 2015/11/19.
  */
object specialSimilarityTag {
  def main(args: Array[String]){
    val conf = new SparkConf()
      .setAppName("specialSimilarityTag")
      .set("spark.rdd.compress", "true")
      .set("spark.storage.memoryFraction", "0.2")
      .set("spark.executor.cores", "2")
      .set("spark.default.parallelism", "1280")
      .set("spark.shuffle.safetyFraction", "0.5")
      .set("spark.executor.extraJavaOptions","-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:PermSize=256M -XX:MaxPermSize=256M")
    val sc = new SparkContext(conf)

    //读取参数并判断参数长度是否满足要求
    args.length match{
      case x if x != 2 => System.err.println("parameter not match")
        System.exit(1)
      case 2 => println("continue")
    }

    val special_tag_file = args(0)
    // val special_tag_file = "/user/hive/warehouse/temp.db/t_special_tag"
    val save_file = args(1)
    // val save_file = "/user/hive/warehouse/analyse.db/t_lxx_special_tag_similarity"
    val temp = sc.textFile(special_tag_file).map{ line =>Try {
      val s = line.split('|')
      (s(1).toInt, s(0),1.0)
    }
    }.filter(_.isSuccess).map(_.get)

    //每个标签出现的次数
    val normDocument = temp.groupBy(_._2).map( x => (x._1,x._2.size))
    //
    val ratings = temp.map( x => (x._2,(x._1,x._3))).join(normDocument).map( x => (x._2._1._1,x._1,x._2._1._2,x._2._2))



    //复制一份tfidf
    val ratings1 = ratings.keyBy(tup => tup._1)
    //两份同样的tfidf进行关联
    val ratingPair = ratings.keyBy(tup => tup._1).join(ratings1).filter( f => f._2._1._2 < f._2._2._2).map(x=>x)
    val vectorCals = ratingPair.map{ f =>
      val key = (f._2._1._2,f._2._2._2)
      val stats =
        ( f._2._1._3 * f._2._2._3,
          f._2._1._4,
          f._2._2._4
        )
      (key,stats)
    }.groupByKey().map{ data =>
      val key = data._1
      val vals = data._2
      val size = vals.size
      val dotProduct = vals.map( f => f._1).sum
      val ratingSq = vals.map(f => f._2).max
      val rating2Sq = vals.map(f => f._3).max
      (key, (size, dotProduct,ratingSq, rating2Sq))
    }
    //定义cosin计算公式
    def cosineSimilarity(dotProduct : Double, ratingNorm : Double, rating2Norm : Double) = {
      dotProduct / (ratingNorm * rating2Norm)
    }

    val similarities = vectorCals.map{ data =>
      val key = data._1
      val (size, dotProduct, ratingNormSq, rating2NormSq) = data._2
      val cosSim = cosineSimilarity(dotProduct, scala.math.sqrt(ratingNormSq), scala.math.sqrt(rating2NormSq))

      (key,  cosSim)
    }

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(save_file), true) } catch { case _ : Throwable => { } }

    similarities.filter(x => x._2 > 0.0).map( x => x._1._1 + "|" + x._1._2 + "|" + x._2).saveAsTextFile(save_file, classOf[GzipCodec])
  }
}
