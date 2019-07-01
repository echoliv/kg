

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, _}
import org.ansj.splitWord.analysis.ToAnalysis
//import org.ansj.util.FilterModifWord
import org.jblas.DoubleMatrix
import scala.util.Try
import scala.collection.JavaConversions._
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.feature.PCA
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql._
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel}
import org.jblas.DoubleMatrix
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.util.Try
object user_song_train {

  def main(args: Array[String]) {
    val exe_overhead = args(0)
    val driver_overhead = args(1)
    val coreNum = args(2)
    val frac = args(3).toDouble
    val rank = args(4).toInt
    val input_file = args(5)
    val save_song_file = args(6)
//    val save_user_file = args(6)
    val behavior_top = 200
    val conf = new SparkConf()
      .setAppName("als")
      .set("spark.rdd.compress", "true")
      .set("spark.executor.cores", coreNum)
      .set("spark.default.parallelism", "1280")
      .set("spark.yarn.executor.memoryOverhead", exe_overhead)
      .set("spark.yarn.driver.memoryOverhead", driver_overhead)
      .set("spark.driver.maxResultSize", "0")
      .set("spark.executor.extraJavaOptions", "-Xloggc:/data1/app/spark_executor_gc_logs/executorGC.log -verbose:gc -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100m -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewRatio=4 -XX:NewRatio=4  -XX:CMSFullGCsBeforeCompaction=5 -XX:+UseCMSCompactAtFullCollection")

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val sc = new SparkContext(conf)
    //val input_file = "/user/hive/warehouse/analyse.db/lej_songkeyindex1_songkeyindex2_confidence"
    //val save_file = "/user/hive/warehouse/analyse.db/lej_songtype_norm_feature/output"
    // save_song_file = "/user/hive/warehouse/temp.db/ll_songid_feaure/output"
    // save_user_file = "/user/hive/warehouse/temp.db/ll_userid_feaure/output"
    val predictuser_file = "/user/hive/warehouse/analyse.db/ll_newsong_predict_userid_songid_rating"


    //读入 用户-歌曲-rating 文件
    val rating = sc.textFile(input_file).map { line => Try {
      val s = line.split('|')
      val user = s(0).toInt
      val songkey = s(1)
      val rating = s(2).toDouble
      (user,songkey, rating)
    }
    }.filter(_.isSuccess).map(_.get).cache()

    //生成训练集合
    val tranins: RDD[Rating] = rating.map(x => Rating(x._1,x._2.toInt,x._3)).sample(false,frac,1)

    //对 用户-歌曲-评分 矩阵进行降维，提取用户、歌曲的行为特征

    val numIterations = 8
    val alpha = 40
    val nonnegative = true
    val model = new ALS().setImplicitPrefs(implicitPrefs = true)
      .setAlpha(alpha)
      .setLambda(0.01)     //正则化系数
      .setRank(rank)
      .setIterations(numIterations)
      .run(tranins)

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
 /*
    //输出用户向量
    val dataDF: RDD[(Int, Array[Double])] = model.userFeatures

    val dataDF1: RDD[(Int, Double, Array[Double])] = dataDF.map{ s =>
      val DM = new DoubleMatrix(s._2)
      val norm = DM.norm2()
      (s._1, norm, s._2)
    }


    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(save_user_file), true)
    } catch {
      case _: Throwable => {}
    }
    dataDF1.map{x =>
      val songid = x._1
      val norm = x._2
      val vec: String = x._3.toList.mkString("|")
      songid + "=" + norm + "=" + vec
    }.saveAsTextFile(save_user_file, classOf[GzipCodec])
*/
    //输出歌曲向量
    val pf: RDD[(Int, Array[Double])] = model.productFeatures

    val datapf1: RDD[(Int, Double, Array[Double])] = pf.map{ s =>
      val DM1 = new DoubleMatrix(s._2)
      val norm = DM1.norm2()

      (s._1, norm, s._2)
    }

    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(save_song_file), true)
    } catch {
      case _: Throwable => {}
    }
    datapf1.map{x =>
      val itemid = x._1
      val norm = x._2
      val vec: String =  x._3.toList.mkString("|")
      itemid + "=" + norm + "=" + vec
    }.saveAsTextFile(save_song_file, classOf[GzipCodec])

  }
}
