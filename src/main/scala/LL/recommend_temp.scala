
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.jblas.{Solve, DoubleMatrix}

import scala.collection.immutable.Map


object recommend_temp {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
    val conf =new SparkConf().setAppName("k-means").setMaster("local").set("spark.driver.allowMultipleContexts","true");
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    computeList(args,sc)
    sc.stop()
  }

  def computeList(args:Array[String],sc:SparkContext): Unit ={
    val recommendHistoryPath = "file\\recommendHistoryPath.txt"
    // read the recommended history data from hive table: mid - songid
    val recommendHistoryData = sc.textFile(recommendHistoryPath).map{ x => x.split('|') }.filter{x=>x.length==2}.map{ x =>
      val user_id = x(0)
      val songlist = x(1).split(',')
      val s = songlist
      (user_id, songlist)
    }

    val ctrRecommendPath = "file\\ctrrecommend.txt"
    // read the recommended history data from hive table: mid - songid
    val ctrRecommendData = sc.textFile(ctrRecommendPath).map{ x => x.split('|') }.filter{x=>x.length==2}.map{ x =>
      val user_id = x(0)
      val songlist = x(1).split(',')
      val s = songlist
      (user_id, songlist)
    }


    val playHistoryPath = "file\\playHistoryPath.txt"
    // read the recommended history data from hive table: mid - songid
    val playHistoryData = sc.textFile(playHistoryPath).map{ x => x.split('|') }.filter{x=>x.length==2}.map{ x =>
      val user_id = x(0)
      val songlist = x(1).split(',')
      val s = songlist
      (user_id, songlist)
    }

    val grouprec = "file\\grouprec.txt"
    val grouprecData = sc.textFile(grouprec).map{ x => x.split('|') }.filter{x=>x.length==2}.map{ x =>
      val user_id = x(0)
      val songlist = x(1).split(',')
      val s = songlist
      (user_id, songlist)
    }
/*
    val newgrouprecData = grouprecData.leftOuterJoin(recommendHistoryData).map { x =>
      val userid = x._1
      val recsongid = x._2._1
      val recfileted = x._2._2
      var s: List[String] = List()
      var songid_to_be_filtered = new Array[String](0)
      if (recfileted != None) {
        songid_to_be_filtered = songid_to_be_filtered.union(recfileted.toList(0))

        var temp = new Array[String](0)
        for (i <- 0 until recsongid.length) {
          var isfalse = 0
          for (j <- 0 until songid_to_be_filtered.length) {
            if (recsongid(i) == songid_to_be_filtered(j)) {
              isfalse = 1
            }
          }
          if (isfalse == 0) {
            s = s :+ recsongid(i).toString
          }
        }
      }
      else {
        for (i <- 0 until recsongid.length) {
          s = s :+ recsongid(i).toString
        }
      }
      var temps = s.toArray
      (userid, temps)
    }
*/
    val newgrouprec = grouprecData.leftOuterJoin(recommendHistoryData).map { x =>
      val userid = x._1
      val recsongid = x._2._1
      val recfileted = x._2._2
      (userid,(recsongid,recfileted))
    }.leftOuterJoin(playHistoryData).map{x =>
      val a1 = x._1
      val a2 = x._2._1._1
      val a3 = x._2._1._2
      val a4 = x._2._2
      (x._1,(x._2._1._1,x._2._1._2,x._2._2))
    }.leftOuterJoin(ctrRecommendData).map{x =>
      val userid = x._1
      val grouprec = x._2._1._1
      val recfilted = x._2._1._2
      val playfilted = x._2._1._3
      val ctrfilted = x._2._2
      var s: List[String] = List()
      var songid_to_be_filtered = new Array[String](0)
      if (recfilted != None) {
        songid_to_be_filtered = songid_to_be_filtered.union(recfilted.toList(0))
      }
      if(playfilted !=None){
        songid_to_be_filtered = songid_to_be_filtered.union(playfilted.toList(0))
      }
      if(ctrfilted != None){
        songid_to_be_filtered = songid_to_be_filtered.union(ctrfilted.toList(0))
      }

      songid_to_be_filtered = songid_to_be_filtered.distinct

      if (songid_to_be_filtered.length >0) {

        var temp = new Array[String](0)
        for (i <- 0 until grouprec.length) {
          var isfalse = 0
          for (j <- 0 until songid_to_be_filtered.length) {
            if (grouprec(i) == songid_to_be_filtered(j)) {
              isfalse = 1
            }
          }
          if (isfalse == 0) {
            s = s :+ grouprec(i).toString
          }
        }
      }
      else {
        for (i <- 0 until grouprec.length) {
          s = s :+ grouprec(i).toString
        }
      }
      (userid, s.toArray)
    }
    newgrouprec.foreach(println)

    val endrec =ctrRecommendData.join(newgrouprec).map{x =>
      val userid = x._1
      val songid1 = x._2._1
      val songid2 = x._2._2
      var endtemp: List[String] = List()
      if (songid2 != None) {
        for (i <- 0 until songid1.length) {
          if (i < songid2.length) {
            endtemp = endtemp :+ songid1(i)
            endtemp = endtemp :+ songid2(i)
          }
          else {
            endtemp = endtemp :+ songid1(i)
          }
        }
      }
      else {
        endtemp  = songid1.toList
      }
      val vec: String =  endtemp.mkString(",")
      (userid,vec)
    }

    endrec.foreach(println)

  }




}