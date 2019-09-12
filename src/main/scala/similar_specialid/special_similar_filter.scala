//过滤相似歌单，如果当前推荐池中存在与之相似的歌单，则歌单不入池
package similar_specialid

import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

import Array._
import scala.collection.mutable.ListBuffer


object special_similar_filter {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
    val conf = new SparkConf().setAppName("k-means").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //读取训练数据

    val special_collectpath = "file\\ctrrecommend.txt"
    val special_specialpath = "file\\badSongidPath.txt"
    val outpath  = "file\\special_feature"

    val special_special = sc.textFile(special_specialpath).map{x=>
      val special1 = x.split('|')(0)
      val special2 = x.split('|')(1)
      (special1,special2)
    }.collect()

    val special_collect = sc.textFile(special_collectpath).map{x=>
      val special = x.split('|')(0)
      val collect_count = x.split('|')(1).toInt
      (special,collect_count)
    }

    val special_1 = sc.textFile(special_specialpath).map{x=>
      val special1 = x.split('|')(0)
      val special2 = x.split('|')(1)
      (special1,special2)
    }.join(special_collect).map(x=> (x._1,(x._2._1,x._2._2)))
      .groupBy(_._1).map{x=>
      val s = x._2.toArray.map{e=> (e._2._1,e._2._2)}.sortBy(_._2).reverse.take(1).map(_._1).toString
      (x._1,s)
    }

    val B_special_collect = sc.broadcast(special_collect)

    val special = sc.textFile(special_collectpath).map{x=>
      val specialid = x.split('|')(0)
      (specialid)
    }.collect()

    var recspecial = new ListBuffer[String]
    val speciallength = special.length
    var deletespecial = new ListBuffer[String]

    for(i<-0 to speciallength-1){
      val tempspecial = special(i)
      val tempspeicalcollect = B_special_collect.value.getOrElse(tempspecial,0)
      var b = 1
      for (j<-0 to special_special.length-1){
        if(special_special(j)._1==tempspecial){
          val similarspecialcollect = B_special_collect.value.getOrElse(special_special(j)._2,0)
          if(tempspeicalcollect>=similarspecialcollect)
            {
              if( !recspecial.contains(tempspecial) && !deletespecial.contains(tempspecial)){
                recspecial +=tempspecial
              }
              if(!deletespecial.contains(special_special(j)._2)){
                deletespecial += special_special(j)._2
              }
            }
          b=0
        }
      }
      if(b==1){
        recspecial+=tempspecial
      }
    }



    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(outpath), true)
    } catch {
      case _: Throwable => {}
    }

  }
}
