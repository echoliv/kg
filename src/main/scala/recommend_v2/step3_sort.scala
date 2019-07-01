
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

import scala.util.Random


object step3_sort {
  def main(args: Array[String]) {

        val conf = new SparkConf()
          .setAppName(args(0))
          .set("spark.rdd.compress", "true")

        val sc = new SparkContext(conf)
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)


        val recommendHistoryPath = args(1)
        val playHistoryPath = args(2)
        val recPath = args(3)
    //    val newhotPath = args(4)
        val preUserSongPath = args(4)
        val yesterdayPath = args(5)
        val hotsongPath = args(6)
        val savePath = args(7)
        //过滤列表获取


/*
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
    val conf =new SparkConf().setAppName("k-means").setMaster("local").set("spark.driver.allowMultipleContexts","true");
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val recommendHistoryPath = "file\\goodSongidPath.txt"
    val playHistoryPath = "file\\grouprec.txt"
    val recPath = "file\\recommend1.txt"
//    val newhotPath = args(4)
    val preUserSongPath = "file\\badSongidPath.txt"
    val yesterdayPath = "file\\goodSongidPath.txt"
    val hotsongPath = "file\\songid.txt"
    val userfeature = "file\\"
    val songfeature = "file\\"
    val savePath = "file\\1226.txt"

*/
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


//    val recnewhotData = sc.textFile(newhotPath).map{x => x.split('=')}.filter{x =>x.length==2}.map{x =>
//      val user_id = x(0)
//      val songlist = x(1).split(',')
//      (user_id,songlist)
//    }

    val preUserdData1 = sc.textFile(preUserSongPath).map{ x => x.split('|')}.map{x=>
      val user_id = x(0)
      val songid = x(1)
      ('a',user_id)
    }.distinct

    val hotsongData = sc.textFile(hotsongPath).map{x =>
      (x.toString)
    }.map{x =>
      ('a',x)
    }

    val hotrandData = preUserdData1.leftOuterJoin(hotsongData).map{x =>
      val userid = x._2._1
      val songid = x._2._2.toList(0)
      val randomNum=(new Random).nextInt(1500)

      (userid,songid,randomNum)
    }.groupBy(_._1).map{row =>
      val songid = row._2.toArray.map{x =>(x._2,x._3)}.sortBy(_._2).take(30).map{x => x._1}.toList
      (row._1,songid)
    }.leftOuterJoin(recommendFilter).map{x =>
      val userid = x._1
      val randrec = x._2._1
      val filter = x._2._2
      var s: List[String] = List()
      if (filter !=None){
        for(i <- 0 until randrec.length){
          if(!filter.contains(randrec(i))){
            s = s :+randrec(i)
          }
        }
      }
      else{
        s = randrec
      }
      val rec = s.distinct
      (x._1,rec)
    }

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(savePath), true)
    } catch {
      case _: Throwable => {}
    }

    hotrandData.map{x =>
      val vec = x._2.mkString(",")
      x._1+'='+vec
    }.saveAsTextFile(savePath)

    sc.stop()
  }
}
