package recall

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map
import scala.collection.immutable.Map
import scala.collection.mutable


object random_walk {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
    val conf =new SparkConf().setAppName("k-means").setMaster("local").set("spark.driver.allowMultipleContexts","true");
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    computeList(args,sc)
    sc.stop()
  }

  def computeList(argc: Array[String], sc:SparkContext): Unit ={

//    val inputPath = argc(1)
//    val outPath = argc(2)
    val inputPath = "file\\personalwalk"
    val outputPath = "file\\personalwalkresult"
    val songidPath = "file\\songid"

    val usersong = sc.textFile(inputPath).map{ x=>
      val userid = x.split('|')(0)
      val songid = x.split('|')(1)
      val score = x.split('|')(2)
      (userid,songid + ":" +score)
    }.reduceByKey((x,y)=> x+","+y)

    val songuser = sc.textFile(inputPath).map{ x=>
      val userid = x.split('|')(0)
      val songid = x.split('|')(1)
      val score = x.split('|')(2)
      (songid,userid + ":" +score)
    }.reduceByKey((x,y)=> x+","+y).union(usersong).collect.toMap

    val songid = sc.textFile(songidPath).map{x =>
      x
    }.collect
    val similarsong = songid.map{ x =>
      val s = personalWalk(songuser,0.85,x,100,songid.toList,sc)
      (x,s)
    }
    similarsong.foreach(println)
  }
  def personalWalk(songuser:scala.collection.immutable.Map[String, String],alpha:Double,root:String,max_step:Int,songid:List[String],sc: SparkContext)={
    var map1 = scala.collection.mutable.Map[String,Double]()
    for(item <- songuser.keys){
      map1 += (item -> 0.0)
    }

    for(i <- 0 to max_step) {
      var tempmap = scala.collection.mutable.Map[String,Double]()
      for(item <- songuser.keys){
        tempmap += (item -> 0.0)
      }
      val result = songuser.map{x=>
        val dangvi = x._1
//        val item = x._2.split(',')
        val songList = x._2
          .split(',')
          .map(e => (e.split(':')(0), e.split(':')(1).toDouble))
        val tempscore = songList.map(e => e._2).sum

        val tempresult = songList.map{ x=>
          val s = map1(dangvi)
          tempmap(x._1) += (map1(dangvi) * alpha / tempscore)
        }
        tempmap(root) += (1-alpha)
        map1 = tempmap
      }
    }
    val re = sc.parallelize(map1.toSeq).filter(e => songid.contains(e._1) && e._1 !=root).sortBy(_._2).collect().reverse.take(100).map(e =>
      (e._1+ ":" +e._2.toString)).reduce((x,y) => x + "," +y)
    re
  }


}

