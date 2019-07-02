package kugou

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, _}
import org.ansj.splitWord.analysis.ToAnalysis
import org.ansj.recognition.impl.StopRecognition
//import org.ansj.library.UserDefineLibrary

import scala.util.Try
import scala.collection.JavaConversions._
/**
  * Created by bearlin on 2015/11/19.
  */
object specialSimilarityIntro {
  def main(args: Array[String]){
    val conf = new SparkConf()
      .setAppName("specialSimilarityIntro")
      .set("spark.rdd.compress", "true")
      .set("spark.storage.memoryFraction", "0.2")
      .set("spark.executor.cores", "2")
      .set("spark.default.parallelism", "1280")
      .set("spark.shuffle.safetyFraction", "0.5")
      .set("spark.executor.extraJavaOptions","-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:PermSize=256M -XX:MaxPermSize=256M")
    val sc = new SparkContext(conf)

    //读取参数并判断参数长度是否满足要求
    args.length match{
      case x if x != 3 => System.err.println("parameter not match")
        System.exit(1)
      case 3 => println("continue")
    }

    val special_file = args(0)
    val stop_file = args(1)
    val save_file = args(2)

    //从文件系统中读取歌单的信息，并对数据进行处理
    val temp = sc.textFile(special_file).map{ line =>Try {
      val s = line.split('|')
      val s1 = s(3).split("@BI_ROW_SPLIT@")
      (s(0).toInt, s(1), s1,s(2))
    }
    }.filter(_.isSuccess).map(_.get).filter(x => x._1 > 0)
    //根据不同的结果做不同的处理
    val documents_1 = temp.filter( x => x._3.length==1).map(x => (x._1,x._4 + ","  + x._3(0)))
    val documents_2 = temp.filter( x => x._3.length==2).map(x => (x._1,x._4))
    val documents_3 = temp.filter( x => x._3.length>=3).map(x => (x._1,x._4 + ","  + x._3( x._3.length - 1 )))
    val documents = (documents_1.union(documents_2)).union(documents_3)
    //对歌单的简介文本进行分词
    val documents_split = documents.map( x => (x._1,ToAnalysis.parse(x._2)))
    //读取停用词表
    val stopwords = new java.util.ArrayList[String]()
    val st = sc.textFile(stop_file).collect
    val size =  st.length
    stopwords.add("gt")
    stopwords.add("lt")
    stopwords.add("br")

    for(i <- 0 to size - 1){
      stopwords.add(st(i))
    }
    val FilterModifWord = new StopRecognition()
    FilterModifWord.insertStopWords(stopwords)
/*


    //根据停用词进行过滤
    val texts = documents_split.map( x => (x._1,FilterModifWord.modifResult(x._2))).map( x => (x._1,x._2.map( y => y.getName)))
    //按照 单词-文档-频数 进行汇总
    val textsTuple = texts.flatMap( x => x._2.toArray.map(y => ((y,x._1),1))).reduceByKey( (x,y) => x+y).map( x => (x._1._1,x._1._2,x._2))
    //每个单词在文档中的出现次数
    val numDocuments = textsTuple.groupBy(_._1).map( x => (x._1,x._2.size)).filter( x=> x._2 > 1)
    //每个文档中的单词数
    val numWords = textsTuple.groupBy(_._2).map( x => (x._1,x._2.size))
    //总文档数
    val n = textsTuple.groupBy(_._2).count.toDouble
    //计算idf
    val idf = numDocuments.map( x => (x._1,math.log( n/x._2)))
    //计算tf-idf
    val tf =  textsTuple.map( x => (x._2,(x._1,x._3))).join(numWords).map(x => (x._2._1._1,x._1,x._2._1._2*1.0/x._2._2) )
    val tfidf = tf.map( x => (x._1,(x._2,x._3))).join(idf).map(x => (x._1,x._2._1._1,x._2._1._2*x._2._2) )
    val normDocument = tfidf.groupBy(_._2).map( x => (x._1,x._2.toArray.map( x => x._3 * x._3 ).sum))
    val tfidf2 = tfidf.map( x => (x._2,(x._1,x._3))).join(normDocument).map( x => (x._2._1._1,x._1,x._2._1._2,x._2._2))
    //复制一份tfidf
    val ratings = tfidf2.keyBy(tup => tup._1)
    //两份同样的tfidf进行关联
    val ratingPair = tfidf2.keyBy(tup => tup._1).join(ratings).filter( f => f._2._1._2 < f._2._2._2)
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

    similarities.filter(x => x._2 > 0.1).map( x => x._1._1 + "|" + x._1._2 + "|" + x._2).repartition(10).saveAsTextFile(save_file, classOf[GzipCodec])
*/
  }
}
