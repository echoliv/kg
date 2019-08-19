//计算歌单特征
//歌曲特征平移
package similar_specialid

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

import Array._


object special_feature_temp1 {

  def arr_format(arr: Array[Double]): String = {
    arr.map(x=>
    {
      val v = x.formatted("%.5f")
      if(v.endsWith(".00000"))
        x.toInt.toString
      else if (v.endsWith("000"))
        x.formatted("%.2f")
      else
        v
    }).mkString("=")
  }

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
    val conf = new SparkConf().setAppName("k-means").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
    val sc = new SparkContext(conf)

/*
    val dt = "2019-08-13"
    val songFeaturePath = "file\\songfeature"
    val songList = "file\\specialsong"
    val special_feature_table = "file\\specialfeature"
    val input_path ="file\\specialtemp"

    val norm_features = args(1)
    val savepath = args(2)
*/
    val sim_song_num = 9
    val input_path = "file\\specialtemp"
    val song_features = "file\\songfeature"
    val special_features ="file\\specialfeature"



    val song_f = sc.textFile(song_features).map { line =>
      val s =line.split('|')
      val id =s(0)
      val feature = s(1).split('=').map(_.toDouble)
      val f_m = new DoubleMatrix(feature)
      val norm = f_m.norm2()
      (id,(norm,feature))
    }.cache()

    val special_f = sc.textFile(special_features).map { line =>
      val s =line.split('|')
      val id =s(0)
      val feature = s(1).split('=').map(_.toDouble)
      val f_m = new DoubleMatrix(feature)
      val norm = f_m.norm2()
      (id,(norm,feature))
    }.cache()

    val (ids,nf) = special_f.collect.unzip
    val norms = new DoubleMatrix(nf.map(_._1))
    val feature = new DoubleMatrix(nf.map(_._2))
    val result_s = song_f.flatMap{x =>
      val songid = x._1
      val songnorm = x._2._1
      val songfeature = new DoubleMatrix(x._2._2)
      val cossims = feature.mmul(songfeature).mmul(1/songnorm).divColumnVector(norms)

      val sort_index = cossims.sortingPermutation().reverse
      val recom_result_nofilter = sort_index.slice(0,sim_song_num).map{e=> (songid,ids(e),cossims.get(e))}
      recom_result_nofilter
    }



    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(input_path), true)
    } catch {
      case _: Throwable => {}
    }

    result_s.map(x =>  (x._1+'|'+x._2+'|'+x._3)).saveAsTextFile(input_path)


  }
}

