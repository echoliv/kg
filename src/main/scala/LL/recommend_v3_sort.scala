import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.{DoubleMatrix, Solve}

import scala.collection.immutable.Map
import scala.util.Random


object recommend_new_v3 {
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
    val yesterdayPath = args(13)

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

    val recYesterdayData=sc.textFile(yesterdayPath).map{ x => x.split('=') }.filter{x=>x.length==2}.map{ x =>
      val user_id = x(0)
      val songlist = x(1).split(',')
      val s = songlist
      (user_id, songlist)
    }

    //过滤列表
    val recommendFilter = preUser.leftOuterJoin(recommendHistoryData).map{x =>
      (x._1,x._2._2)
    }.leftOuterJoin(playHistoryData).map{x =>

      (x._1,(x._2._1,x._2._2))
    }.leftOuterJoin(recYesterdayData).map{x =>
      val user_id = x._1
      val rechistory = x._2._1._1
      val recplay = x._2._1._2
      val recyes = x._2._2
      var songid_to_be_filtered = new Array[String](0)
      if (rechistory != None) {
        songid_to_be_filtered = songid_to_be_filtered.union(rechistory.toList(0))
      }
      if(recplay !=None){
        songid_to_be_filtered = songid_to_be_filtered.union(recplay.toList(0))
      }
      if(recyes !=None){
        songid_to_be_filtered = songid_to_be_filtered.union(recyes.toList(0))
      }

      songid_to_be_filtered = songid_to_be_filtered.distinct
      (user_id,songid_to_be_filtered)
    }

    //召回池导入
    //歌单
    val specialData1 = sc.textFile(specialRecPath).map{ x => x.split('|')}.map{x=>
      val songid1 = x(0)
      val temps = x(1)+','+x(2).toString+','+x(3).toString
      (songid1,temps)
    }

    //主题
    val themeData2 = sc.textFile(themeRecPath).map{x =>x.split('|')}.map{x =>
      val songid1 = x(0)
      val songid2 = x(1)
      val sim = x(2).toDouble
      val rank = x(3).toInt
      (songid1,songid2+','+sim+','+rank)
    }

    //语言(除国语粤语)
    val languageData3 = sc.textFile(languageRecPath).map{x =>x.split('|')}.map{x =>
      val songid1 = x(0)
      val songid2 = x(1)
      val sim = x(2).toDouble
      val rank = x(3).toInt
      (songid1,songid2+','+sim+','+rank)
    }

    //歌手
    val singerData4 = sc.textFile(singerRecPath).map{x =>x.split('|')}.map{x =>
      val songid1 = x(0)
      val songid2 = x(1)
      val sim = x(2).toDouble
      val rank = x(3).toInt
      (songid1,songid2+','+sim+','+rank)
    }

    //语言音频
    val audioData5 = sc.textFile(languageAudioRecPath).map{x =>x.split('|')}.map{x =>
      val songid1 = x(0)
      val songid2 = x(1)
      val sim = x(2).toDouble
      val rank = x(3).toInt
      (songid1,songid2+','+sim+','+rank)
    }

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

    val recSpecial1 = preUserSong.join(specialData1).map{x =>
      val songid1 = x._1
      val userid = x._2._1
      val info = x._2._2.toString.split(',')
      val songid2 = info(0)
      val sim = info(1)
      val rank = info(2)
      val randomNum=(new Random).nextInt(2000)
      (userid,songid1,songid2,sim,rank,randomNum)
    }.filter{x => x._5.toInt <100}.groupBy(_._1).map{row =>
      val songid = row._2.toArray.map{x => (x._3,x._4,x._6)}.sortBy(_._2).reverse.take(150).sortBy(_._3).take(50).map{x => x._1}
      (row._1,songid.distinct)
    }

    val recTheme2 = preUserSong.join(themeData2).map{x =>
      val songid1 = x._1
      val userid = x._2._1
      val info = x._2._2.toString.split(',')
      val songid2 = info(0)
      val sim = info(1)
      val rank = info(2)
      (userid,songid1,songid2,sim,rank)
    }.filter{x => x._5.toInt <5}.groupBy(_._1).map{row =>
      val songid = row._2.toArray.map{x => (x._3,x._4)}.sortBy(_._2).reverse.take(10).map{x => x._1}
      (row._1,songid.distinct)
    }

    val recLanguage3 = preUserSong.join(languageData3).map{x =>
      val songid1 = x._1
      val userid = x._2._1
      val info = x._2._2.toString.split(',')
      val songid2 = info(0)
      val sim = info(1)
      val rank = info(2)
      val randomNum=(new Random).nextInt(1000)
      (userid,songid1,songid2,sim,rank,randomNum)
    }.filter{x => x._5.toInt <50}.groupBy(_._1).map{row =>
      val songid = row._2.toArray.map{x => (x._3,x._4,x._6)}.sortBy(_._2).reverse.take(150).sortBy(_._3).take(30).map{x => x._1}
      (row._1,songid.distinct)
    }

    val recSinger4 = preUserSong.join(singerData4).map{x =>
      val songid1 = x._1
      val userid = x._2._1
      val info = x._2._2.toString.split(',')
      val songid2 = info(0)
      val sim = info(1)
      val rank = info(2)
      (userid,songid1,songid2,sim,rank)
    }.filter{x => x._5.toInt <20}.groupBy(_._1).map{row =>
      val songid = row._2.toArray.map{x => (x._3,x._4)}.sortBy(_._2).reverse.take(6).map{x => x._1}
      (row._1,songid.distinct)
    }

    val recAudio5 = preUserSong.join(audioData5).map{x =>
      val songid1 = x._1
      val userid = x._2._1
      val info = x._2._2.toString.split(',')
      val songid2 = info(0)
      val sim = info(1)
      val rank = info(2)
      (userid,songid1,songid2,sim,rank)
    }.groupBy(_._1).map{row =>
      val songid = row._2.toArray.map{x => (x._3,x._4)}.sortBy(_._2).reverse.take(5).map{x => x._1}
      (row._1,songid.distinct)
    }


    val rec1_5 =preUser.leftOuterJoin(recSpecial1).map{x =>
      val userid = x._1
      val songidlist = x._2._2
      (userid,songidlist)
    }.leftOuterJoin(recTheme2).map{x =>
      (x._1,(x._2._1,x._2._2))
    }.leftOuterJoin(recLanguage3).map{x =>
      (x._1,(x._2._1._1,x._2._1._2,x._2._2))
    }.leftOuterJoin(recSinger4).map{x =>
      (x._1,(x._2._1._1,x._2._1._2,x._2._1._3,x._2._2))
    }.leftOuterJoin(recAudio5).map{x =>
      (x._1,(x._2._1._1,x._2._1._2,x._2._1._3,x._2._1._4,x._2._2))

    }.leftOuterJoin(recommendFilter).map{x =>
      val userid = x._1
      val v1special = x._2._1._1
      val v2theme = x._2._1._2
      val v3language = x._2._1._3
      val v4singer = x._2._1._4
      val v5audio = x._2._1._5
      val filtered = x._2._2

      var recommendv1 = new Array[String](0)
      var s1: List[String] = List()
      var s2: List[String] = List()

      if (v1special != None) {
        recommendv1 = recommendv1.union(v1special.toList(0))
      }
      if (v2theme != None){
        recommendv1 = recommendv1.union(v2theme.toList(0))
      }
      if (v3language != None){
        recommendv1 = recommendv1.union(v3language.toList(0))
      }
      if (v4singer != None){
        recommendv1 = recommendv1.union(v4singer.toList(0))
      }
      if (v5audio != None){
        recommendv1 = recommendv1.union(v5audio.toList(0))
      }
      recommendv1 = recommendv1.distinct

      if(filtered !=None){
        for(i <-0 until recommendv1.length){
          if(!filtered.contains(recommendv1(i))){
            s1 = s1 :+recommendv1(i).toString
          }
        }
      }
      else {
        s1 = recommendv1.toList
      }
      (userid,s1)
    }

    val recBehavior6 = preUserSong.join(behaviorData6).map{x =>
      val songid1 = x._1
      val userid = x._2._1
      val info = x._2._2.toString.split(',')
      val songid2 = info(0)
      val sim = info(1)
      val rank = info(2)
      val randomNum=(new Random).nextInt(4000)
      (userid,songid1,songid2,sim,randomNum,rank)
    }.filter{x => x._5.toInt <100}.groupBy(_._1).map{row =>
      val songid = row._2.toArray.map{x => (x._3,x._4,x._5)}.sortBy(_._2).reverse.take(400).sortBy(_._3).take(200).map{x => x._1}
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
      (userid,s)
    }
    recBehavior6.foreach(println)

    val alluserrec =preUser.leftOuterJoin(rec1_5).map{x =>
      (x._1,x._2._2)
    }.leftOuterJoin(recBehavior6).map { x =>
      val userid = x._1
      val firstrec = x._2._1
      val lastrec = x._2._2
      var s: List[String] = List()


      if (firstrec != None) {
        val firstarray = firstrec.toArray
        var i = 0
        while (i<firstrec.toList(0).size) {
          s = s :+firstrec.get(i)
          i = i+1
        }
      }
      if (lastrec != None) {
        //        val lastarray = lastrec.toArray
        var temp0 = lastrec.toList(0).size
        if (lastrec.toList(0).size < temp0) {
          temp0 = lastrec.toList(0).size
        }
        for (j <- 0 until temp0) {

          s = s :+lastrec.get(j)
        }
      }
      (userid,s)
    }
    alluserrec.foreach(println)

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(outputPath1), true)
    } catch {
      case _: Throwable => {}
    }

    alluserrec.map{x =>
      val vec = x._2.mkString(",")
      x._1+'='+vec
    }.saveAsTextFile(outputPath1)
  }
}