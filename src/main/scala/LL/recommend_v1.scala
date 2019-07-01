import org.apache.hadoop.io.compress.GzipCodec
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD


object recommend_v1 {
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


    val groupRecPath = args(1)
    val ctrRecPath = args(2)
    val playHistoryPath = args(3)
    val recommendHistoryPath = args(4)
    val outputPath = args(5)

    val recommendHistoryData = sc.textFile(recommendHistoryPath).map{ x => x.split('=') }.filter{x=>x.length==2}.map{ x =>
      val user_id = x(0)
      val songlist = x(1).split(',')
      val s = songlist
      (user_id, songlist)
    }

    val ctrRecommendData = sc.textFile(ctrRecPath).map{ x => x.split('=') }.filter{x=>x.length==2}.map{ x =>
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

    val grouprecData = sc.textFile(groupRecPath).map{ x => x.split('|') }.filter{x=>x.length==2}.map{ x =>
      val user_id = x(0)
      val songlist = x(1).split(',')
      val s = songlist
      (user_id, songlist)
    }


    val newgrouprec = grouprecData.leftOuterJoin(recommendHistoryData).map { x =>
      (x._1,(x._2._1,x._2._2))
    }.leftOuterJoin(playHistoryData).map{x =>
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

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(outputPath), true)
    } catch {
      case _: Throwable => {}
    }

    ctrRecommendData.join(newgrouprec).map{x =>
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
      userid + '=' + vec
    }.saveAsTextFile(outputPath)

  }
}