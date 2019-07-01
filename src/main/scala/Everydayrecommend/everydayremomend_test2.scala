
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object everydayrecommend_test2 {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("everydayrecommend_test")
      .set("spark.rdd.compress", "true")
    /*
   System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
   val conf =new SparkConf().setAppName("k-means").setMaster("local").set("spark.driver.allowMultipleContexts","true");
   */
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    recommendList(args,sc)
    sc.stop()
  }

  def recommendList(argc: Array[String], sc: SparkContext): Unit ={


    val filterpath1 = argc(0)
    val filterpath2 = argc(1)
    val songid_filter_path = argc(2)
    val songid_invalid_path = argc(3)
    val userid_songid_path = argc(4)
    val userid_rec30_path = argc(5)
    val userid_cache_path = argc(6)
    val savepath1 = argc(7)
    val savepath2 = argc(8)
    val savepath3 = argc(9)
    val recnum = argc(10).toInt
    /*

        val filterpath1 = "file\\everydayrecommend_test1\\name_1"
        val filterpath2 = "file\\everydayrecommend_test1\\name_2"
        val songid_filter_path ="file\\everydayrecommend_test1\\name_3"
        val userid_songid_path = "file\\everydayrecommend_test1\\name_5"
        val songid_invalid_path = "file\\everydayrecommend_test1\\name_4"
        val userid_rec30_path = "file\\everydayrecommend_test1\\name_6"
        val userid_cache_path = "file\\everydayrecommend_test1\\name_6"
        val savepath1 = "file\\everydayrecommend_test1\\name_7"
        val savepath2 = "file\\everydayrecommend_test1\\name_8"
        val savepath3 = "file\\everydayrecommend_test1\\name_9"
        val recnum = 2
    */
    val user_song = sc.textFile(userid_songid_path).map{x => x.split('|')}.map{x =>
      val userid = x(0)
      val songid = x(1)
      (songid ,userid)
    }

    val songid_filter = sc.textFile(songid_filter_path).map{x=>
      (x.toString,1)
    }

    val songid_invalid = sc.textFile(songid_invalid_path).map{x =>
      (x.toString,1)
    }.union(songid_filter).distinct

    val filter1 = sc.textFile(filterpath1).map{x => x.split('=')}.map{x =>
      val userid = x(0)
      val songlist = x(1).split(',')
      (userid,songlist)
    }
    val filter2 = sc.textFile(filterpath2).map{x => x.split('=')}.map{x =>
      val userid = x(0)
      val songlist = x(1).split(',')
      (userid,songlist)
    }

    val userid_recommend = sc.textFile(userid_rec30_path).map{x => x.split('=')}.map{x =>
      val userid = x(0)
      val songlist =x(1).split(',')
      (userid,songlist)
    }

    //过滤列表
    val filter = filter1.leftOuterJoin(filter2).map{x=>
      (x._1,(x._2._1,x._2._1))
    }.leftOuterJoin(userid_recommend).map{x =>
      val userid = x._1
      val filtersongid1 = x._2._1._1
      val filtersongid2 = x._2._1._2
      val filtersongid3 = x._2._2
      var songid_filter = new Array[String](0)
      if (filtersongid1 !=None){
        songid_filter = songid_filter.union(filtersongid1)
      }
      if (filtersongid2 !=None){
        songid_filter = songid_filter.union(filtersongid2)
      }
      if (filtersongid3 !=None){
        songid_filter = songid_filter.union(filtersongid3.toList(0))
      }
      songid_filter = songid_filter.distinct
      (userid,songid_filter)
    }


    //歌曲过滤
    val recommend_filter1 = user_song.leftOuterJoin(songid_invalid).map{x =>
      val songid = x._1
      val userid = x._2._1
      val temp1 = x._2._1
      (userid,songid,temp1)
    }.filter{x => x._3 != 1}.map{x =>
      (x._1,x._2)
    }.leftOuterJoin(filter).filter{x=> x._2._2 !=1}.map{x =>
      val temp2 = scala.util.Random.nextInt(100)
      (x._1,x._2._1,temp2)
    }.groupBy(_._1).map{row =>
      val songid = row._2.toArray.map{x => (x._2,x._3)}.sortBy(_._2).take(recnum).map{x => x._1}

      (row._1,songid)

    }

    val rec1 = sc.textFile(userid_rec30_path).map{x =>x.split('=')}.map{x =>
      (x(0),x(1))
    }.leftOuterJoin(recommend_filter1).map{x =>
      val userid = x._1
      var rec30 = x._2._1.toString.split(',')
      val rec30len = rec30.length
      val rec2 = x._2._2

      if(rec2 !=None){
        if (rec2.get.length == 1 && !rec30.contains(rec2.get(0))){

          rec30(rec30len-1) = rec2.get(0)
        }
        if(rec2.get.length == 2){
          if(!rec30.contains(rec2.get(1))) {
            rec30(rec30len-2) = rec2.get(1)
          }
          if(!rec30.contains(rec2.get(0))) {
            rec30(rec30len - 1) = rec2.get(0)
          }
        }
      }
      (userid,rec30)
    }
/*
    val rec2 = sc.textFile(userid_cache_path).map{x =>x.split('=')}.map{x =>
      (x(0),x(1))
    }.leftOuterJoin(recommend_filter1).map{x =>
      val userid = x._1
      var rec30 = x._2._1.toString.split(',')
      val rec30len = rec30.length
      val rec2 = x._2._2

      if(rec2 !=None){
        if (rec2.get.length == 1 && !rec30.contains(rec2.get(0))){

          rec30(rec30len-1) = rec2.get(0)
        }
        if(rec2.get.length == 2){
          if(!rec30.contains(rec2.get(1))) {
            rec30(rec30len-2) = rec2.get(1)
          }
          if(!rec30.contains(rec2.get(0))) {
            rec30(rec30len - 1) = rec2.get(0)
          }
        }
      }
      (userid,rec30)
    }
*/
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
/*
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(savepath1), true)
    } catch {
      case _: Throwable => {}
    }

    rec2.map{x =>
      val vec = x._2.toList.mkString(",")
      x._1+'='+vec
    }.saveAsTextFile(savepath1)
*/
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(savepath2), true)
    } catch {
      case _: Throwable => {}
    }

    rec1.map{x =>
      val vec = x._2.toList.mkString(",")
      x._1+'='+vec
    }.saveAsTextFile(savepath2)

    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(savepath3), true)
    } catch {
      case _: Throwable => {}
    }

    recommend_filter1.map{x =>
      val vec = x._2.toList.mkString(",")
      x._1+'='+vec
    }.saveAsTextFile(savepath3)
  }
}

