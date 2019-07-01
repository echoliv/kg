
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

import scala.util.Random


object step4_sort3 {
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
    //    val recnewhotData = sc.textFile(newhotPath).map{x => x.split('=')}.filter{x =>x.length==2}.map{x =>
    //      val user_id = x(0)
    //      val songlist = x(1).split(',')
    //      (user_id,songlist)
    //    }

    val hotrandData = sc.textFile(playHistoryPath).map{x => x.split('=')}.filter{x =>x.length==2}.map{x =>
      val user_id = x(0)
      val songlist = x(1).split(',')
      (user_id,songlist)
    }

    val recData = sc.textFile(recPath).map{x => x.split('=')}.filter{x =>x.length==2}.map{x =>
      val user_id = x(0)
      val songlist = x(1).split(',')
      (user_id,songlist)
    }
    //推荐列表
    val endrecData = recData.join(hotrandData).map{x =>
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

      (userid,endtemp.distinct)
    }
    /*
        //排序选择歌曲
        val newrecommend = sc.textFile(recPath).map{x => x.split('=')}.map{x =>
          val userid = x(0)
          val songlist=x(1)

          (userid,songlist)
        }.flatMapValues(_.split(",")).map{x =>
          val userid = x._1
          val songid = x._2
          (userid,songid)
        }.join(userf).map{x =>
          (x._2._1,(x._2._2,x._1))
        }.join(songf).map{x =>
          val songid = x._1
          val userid = x._2._1._2
          val songfeat= new DoubleMatrix(x._2._2.split(',').toList(0))
          val userfeat = new DoubleMatrix(x._2._1._1.split(',').toList(0))
          val normsong = songfeat.norm2()
          val normuser = userfeat.norm2()
        }

        */

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(savePath), true)
    } catch {
      case _: Throwable => {}
    }

    endrecData.map{x =>
      val vec = x._2.mkString(",")
      x._1+'='+vec
    }.saveAsTextFile(savePath)

    sc.stop()
  }
}
