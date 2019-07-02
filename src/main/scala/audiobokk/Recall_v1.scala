package audiobokk

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.{DoubleMatrix, Solve}

import scala.collection.immutable.Map
import scala.tools.nsc.interpreter.Completion.Candidates
object Audio_recall_v1 {
  def main(args: Array[String]) {
   /*
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
    val conf = new SparkConf().setAppName("k-means").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
    val sc = new SparkContext(conf)
    val specialRecPath = "file\\badSongidPath.txt"

    */
    val conf = new SparkConf()
      .setAppName(args(0))
      .set("spark.rdd.compress", "true")

    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    computeList(args, sc)
    sc.stop()
  }
  def computeList(args: Array[String], sc: SparkContext): Unit = {

    val user_album_path = args(1)   //用户推荐源
    val similar_album_path = args(2)  //召回池
    val user_filter_path = args(3)  //用户过滤列表
    //    val album_feature_path = args(3) //专辑特征
    val user_feature_path  = args(4)  //用户分类统计
    val tag_album_path = args(5)   //专辑_标签
    //    val time_album_path =args(6)  //专辑_发版时间
    val name_album_path =args(6)    //专辑_歌手
    val comple_path = args(7)
    val input_path = args(8)      //输出路径
    val min_num = args(9).split('_')(0).toInt    //每个专辑召回个数
    val recommend_num =args(9).split('_')(1).toInt  //每个用户推荐个数
/*
    val user_album_path="file\\audio\\user_album_path"
    val similar_album_path ="file\\audio\\similar_album_path"
    val user_filter_path ="file\\audio\\user_filter_path"
    val user_feature_path="file\\audio\\user_feature_path"
    val tag_album_path="file\\audio\\tag_album_path"
    val name_album_path="file\\audio\\name_album_path"
    val comple_path ="file\\audio\\comple_path"
    val input_path ="file\\audio\\input_path"
*/
//召回池
    var similar_album = sc.textFile(similar_album_path).map{x=>x.split('|')}.map{x=>
    (x(0),x(1)+':'+x(2))
    }.reduceByKey((x,y)=>x+','+y).collect.toMap
    val B_similar = sc.broadcast(similar_album)
    similar_album=null

    //热门补充
    var comple_data = sc.textFile(comple_path).map(x=> x.split('|')).map{x=>
      (x(0),x(1))
    }.groupBy(_._1).map{row=>
      val tag = row._1
      val comlist = row._2.map(x=>x._2).toArray
      (tag,comlist)
    }.collect.toMap
    val B_comple=sc.broadcast(comple_data)
    comple_data =null

    //用户过滤列表
    val filter_list = sc.textFile(user_filter_path).map{x=> x.split('|')}.map{x=>
      val albumidlist = x(1).split(',')
      (x(0),albumidlist)
    }

    //专辑_标签
    var tag_album = sc.textFile(tag_album_path).map{x=>x.split('|')}.map{x=>
      (x(0),x(1))
    }.collect.toMap
    val B_tag_album = sc.broadcast(tag_album)
    tag_album = null
    //专辑_发版时间
    //    var time_album = sc.textFile(time_album_path).map{x=>x.split('|')}.map{x=>
    //      (x(0),x(1))
    //    }
    //    val B_time_album = sc.broadcast(time_album)
    //    time_album = null
    //用户标签特征
    val user_tag_feature = sc.textFile(user_feature_path).map{x=> x.split('|')}.map{x=>
      (x(0),x(1))
    }
    //专辑是否同意节目、书籍等
    var name_album= sc.textFile(name_album_path).map{x=>
      x.split('|')
    }.map{x=>
      (x(0),x(1))
    }.collect.toMap
    val B_name_album = sc.broadcast(name_album)
    name_album =null

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(input_path), true)
    } catch {
      case _: Throwable => {}
    }

    //(userid,albumid,score,filter,tagfeature)
    val incrementRating = sc.textFile(user_album_path).map { x => x.split('|') }
      .map { x => (x(0), x(1), x(2).toDouble)}.groupBy(_._1).map{x=>
      val (albumid,value) = x._2.map(e =>(e._2,e._3)).unzip

      (x._1,(albumid.toArray,value.toArray))
    }.leftOuterJoin(filter_list).map{x=>
      (x._1,(x._2._1._1,x._2._1._2,x._2._2))
    }.join(user_tag_feature).map{x=>
      val userid =x._1
      val albumidlist = x._2._1._1
      val scorelist = x._2._1._2
      val filter = x._2._1._3
      val tagfeature = x._2._2.toString.replace("Some(","").replace(")","").split('=')

      var songlist2:scala.collection.mutable.Map[String, Double] = scala.collection.mutable.Map()
      var j =0
      //      val album = sc.parallelize(albumidlist.zip(scorelist)).join(similar_album).map{x=>
      //        (x._2._2._1,(1-(x._2._1-1)*0.3)*x._2._2._2)}
      for(i<-0 to albumidlist.length-1) {
        val tf = B_similar.value.get(albumidlist(i)).getOrElse("no")
        if (tf !="no") {
          val list1 = B_similar.value(albumidlist(i)).split(',')
          for(i1 <-0 to list1.length-1){
            val albumid = list1(i1).split(':')(0)
            val score = list1(i1).split(':')(1).toDouble
            if(filter!=None) {
              if (!filter.get.contains(albumid)) {
                if (!songlist2.contains(albumid)) {
                  songlist2 += (albumid -> (1 - (scorelist(i) - 1) * 0.3) * score)
                } else {
                  songlist2(albumid) = songlist2(albumid) * 0.6 + (1 - (scorelist(i) - 1) * 0.3) * 0.6 * score
                }
              }
            }else
            {
              if (!songlist2.contains(albumid)) {
                songlist2 += (albumid -> (1 - (scorelist(i) - 1) * 0.3) * score)
              } else {
                songlist2(albumid) = songlist2(albumid) * 0.6 + (1 - (scorelist(i) - 1) * 0.3) * 0.6 * score
              }
            }
          }
        }
      }
      val re = songlist2.toList.sortBy(-_._2).take(100).toArray
      val result =get_recommend(re,tagfeature ,B_name_album.value,B_tag_album.value,B_comple.value,recommend_num,min_num)
      (userid+'='+result.mkString(",").replace(",null",""))
    }.saveAsTextFile(input_path)
  }

  def get_recommend(canditation: Array[(String, Double)], tagfeature: Array[String], name_album: Map[String, String], tag_album: Map[String, String],comple:Map[String,Array[String]],recommend_num:Int,min_num:Int):Array[String]={
    var tagCount: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.Map()
    var nameCount:scala.collection.mutable.Map[String, Int] = scala.collection.mutable.Map()
    var songlist = new Array[String](recommend_num)
    var j =0
    var flag=true
    for(i<-0 to canditation.length-1 if flag) {
      val albumid = canditation(i)._1
      //      val tag = tag_album(albumid)
      val name = name_album.get(albumid).getOrElse("no")
      if(name!="no") {
        if (!nameCount.contains(name)) {
          nameCount += (name -> 1)
          songlist(j) = canditation(i)._1+':'+canditation(i)._2.formatted("%.5f").toString
          j += 1
        } else {
          if (nameCount(name) < 2) {
            nameCount(name) += 1
            songlist(j) = canditation(i)._1+':'+canditation(i)._2.formatted("%.5f").toString
            j += 1
          }
        }
      }else {
        songlist(j) = canditation(i)._1+':'+canditation(i)._2.formatted("%.5f").toString
        j += 1
      }
      if (j >= recommend_num)
        flag = false
    }
    if(j<min_num)
    {

      var bigtag =0
      for(t1 <- 1 to tagfeature.length-1){
        if(tagfeature(bigtag).toDouble<tagfeature(t1).toDouble)
          bigtag =t1
      }
      for(z<-0 to min_num-j-1){
        val rand1 = scala.util.Random.nextInt(50)
        songlist(j) =comple(bigtag.toString)(rand1)+':'+'0'
      }
    }
    return songlist
  }
}