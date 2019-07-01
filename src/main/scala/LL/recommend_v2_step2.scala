import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.jblas.{Solve, DoubleMatrix}

import scala.collection.immutable.Map


object recommend_new_v2_step2 {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(args(0))
      .set("spark.rdd.compress", "true")

    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    computeList(args, sc)
    sc.stop()
  }

  def computeList(args:Array[String],sc:SparkContext): Unit ={


    val themeRecPath = args(1)
    val singerRecPath = args(2)
    val behaviorRecPath = args(3)
    val languageRecPath = args(4)
    val specialRecPath = args(5)
    val languageAudioRecPath = args(6)
    val recommendHistoryPath = args(7)
    val playHistoryPath = args(8)
    val outputPath1 = args(9)
    val outputPath2 = args(10)
    val outputPath = args(11)
    val preUserSongPath = args(12)


    //过滤列表获取

    val preUser = sc.textFile(preUserSongPath).map{ x => x.split('|')}.map{x=>
      val user_id = x(0)
      val songid = x(1)
      (user_id,1)
    }.distinct

    val recommendHistoryData = sc.textFile(recommendHistoryPath).map{ x => x.split('=') }.filter{x=>x.length==2}.map{ x =>
      val user_id = x(0)
      val songlist = x(1).split(',')
      val s = songlist
      (user_id, songlist)
    }

    val playHistoryData = sc.textFile(playHistoryPath).map{ x => x.split('=') }.filter{x=>x.length==2}.map{ x =>
      val user_id = x(0)
      val songlist = x(1).split(',')
      val s = songlist
      (user_id, songlist)
    }

    //过滤列表
    val recommendFilter = preUser.leftOuterJoin(recommendHistoryData).map{x =>
      (x._1,x._2._2)
    }.leftOuterJoin(playHistoryData).map{x =>
      val user_id = x._1
      val rechistory = x._2._1
      val recplay = x._2._2
      var songid_to_be_filtered = new Array[String](0)
      if (rechistory != None) {
        songid_to_be_filtered = songid_to_be_filtered.union(rechistory.toList(0))
      }
      if(recplay !=None){
        songid_to_be_filtered = songid_to_be_filtered.union(recplay.toList(0))
      }

      songid_to_be_filtered = songid_to_be_filtered.distinct
      (user_id,songid_to_be_filtered)
    }
    var filtermap = recommendFilter.collect.toMap
    val B_filtermap = sc.broadcast(filtermap)

    //召回池导入


    //行为
    val behaviorData6 = sc.textFile(behaviorRecPath).map{x =>x.split('|')}.map{x =>
      val songid1 = x(0)
      val songid2 = x(1)
      val sim = x(2).toDouble
      val rank = x(3).toInt
      (songid1,songid2+','+sim+','+rank)
    }

    //用户推荐源
    val preUserSong = sc.textFile(preUserSongPath).map{ x => x.split('|')}.map{x=>
      val user_id = x(0)
      val songid = x(1)
      (songid,user_id)
    }

      val recBehavior6 = preUserSong.join(behaviorData6).map{x =>
      val songid1 = x._1
      val userid = x._2._1
      val info = x._2._2.toString.split(',')
      val songid2 = info(0)
      val sim = info(1)
      val rank = info(2)
      (userid,songid1,songid2,sim,rank)
    }.filter{x => x._5.toInt <50}.groupBy(_._1).map{row =>
      val songid = row._2.toArray.map{x => (x._2,x._3)}.sortBy(_._2).reverse.take(200).map{x => x._1}
      (row._1,songid.distinct)
    }.leftOuterJoin(recommendFilter).map{x =>
      val userid =x._1
      val behavior = x._2._1
      val filtered = x._2._2
      var recommendv1 = new Array[String](0)
      var s: List[String] = List()

      if(filtered != None){
        for (i <- 0 until behavior.length){
          if(!filtered.contains(behavior(i))){
            s = s :+behavior(i).toString
          }
        }
      }
      s = behavior.toList
      (userid,s.toArray)
    }

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(outputPath2), true)
    } catch {
      case _: Throwable => {}
    }
    recBehavior6.map{x =>
      val vec = x._2.toList.mkString(",")
      x._1+'='+vec
    }.saveAsTextFile(outputPath2)

  }
}