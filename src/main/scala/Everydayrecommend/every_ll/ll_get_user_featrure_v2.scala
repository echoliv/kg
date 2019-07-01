//用户特征计算
package Everydayrecommend.every_ll

import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix
import Array._

object ll_get_user_featrure_v2 {

  def arr_format(arr: Array[Double]): String = {
    arr.map(x => {
      val v =x.formatted("%.5f")
      if (v.endsWith(".00000"))
        x.toInt.toString
      else if(v.endsWith(".000"))
        x.formatted("%.2f")
      else
        v
    }).mkString("=")
  }

  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder().appName("")
      .config("spark.rdd.compress","true")
      .config("","")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val silceFinal = 73
    val per =0.8
    val dt = args(0)
    val songFeaturePath = args(1)
    val favorHistoryPath = args(2)
    val user_feature_table = args(3)

    val favorhistory =sc.textFile(favorHistoryPath).map{
      x =>
        val userid = x.split('|')(0)
        val songid = x.split('|')(1)
        val rating = x.split('|')(2)
        (userid,songid +":"+ rating)
    }.reduceByKey{(x,y)=> x +","+ y
    }

    favorhistory.take(3).foreach(println)
    favorhistory.persist()

    val songfeature = sc.textFile(songFeaturePath).map{
      x =>
        (x.split('|')(0),x.split('|')(1).split('=').map{e => e.toDouble})
    }.collectAsMap()

    val songfeaturelength = songfeature.take(1).values.toList.head.length

    val songFeature_b = sc.broadcast(songfeature)

    //compute userfeature
    val userfeature = favorhistory.map{ x =>
      val userid = x._1
      val songAndScoreList = x._2.split(',')
        .map{e => (e.split(',')(0),e.split(',')(1).toDouble)}
        .filter(e => songFeature_b.value.contains(e._1))

      val scoreSum = songAndScoreList.map(x => x._2).sum
      var feature = DoubleMatrix.zeros(songfeaturelength)
      for ((songid,score) <- songAndScoreList) {
        val songfeature = new DoubleMatrix(songFeature_b.value.getOrElse(songid,DoubleMatrix.zeros(songfeaturelength).toArray))
        feature = if (scoreSum>0) songfeature.mmul(score*1.0/scoreSum).add(feature) else DoubleMatrix.zeros(songfeaturelength)
      }

      val feature_d = feature.getRange(0,73).toArray
      val feature_other = feature.getRange(73,songfeaturelength).toArray
      val feature_c = feature_d.map(e => if (e>0.8) 1.0 else 0.0)
      (userid,arr_format(concat(feature_d,feature_c,feature_other)))
    }
    userfeature.toDF("user_id","user_features").createOrReplaceTempView("tmp")
    val sql =
      """
        insert overwrite table %s partition (dt = %s)
        select user_id,user_features
        from tmp
      """.format(user_feature_table,dt)
    spark.sql(sql)
  }
}
