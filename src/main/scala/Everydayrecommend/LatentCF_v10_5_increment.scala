package song_recomend.common_user

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.jblas.{Solve, DoubleMatrix}

import scala.collection.immutable.Map


object LatentCF_v10_6_increment {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("LatentCF_v10_6_increment_" + args(0))
      .set("spark.rdd.compress", "true")

    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    computeList(args, sc)
    sc.stop()
  }

  def computeList(args: Array[String], sc: SparkContext): Unit = {

    // data source path and MF parameters
    val trainDataPath = args(1)
    val incrementDataPath = args(2)
    val playHistoryPath = args(3)
    val recommendHistoryPath = args(4)
    val songidSingerPath = args(5)
    val goodSongidPath = args(6)
    val badSongidPath = args(7)
    val similarSongPath = args(8)
    val specialSongPath = args(9)
    val songIDFPath = args(10)
    val userInfoPath = args(11)
    val productFeaturePath = args(12)
    val savePath = args(13)
    val mf_parameter = args(14)

    // setting the MF parameters
    val alpha = mf_parameter.split('_')(0).toDouble
    val lambda = mf_parameter.split('_')(1).toDouble
    val rank = mf_parameter.split('_')(2).toInt

    // songid to be preserved
    val goodSongid = sc.textFile(goodSongidPath).map { x => (x.toString, 1) }

    // songid to be filtered
    val badSongid = sc.textFile(badSongidPath).map { x => (x.toString, 1) }

    // training data
    val trainRating = sc.textFile(trainDataPath).map(line => line.split('|')).map { line =>
      try {
        (line(0), line(1), line(2).toDouble)
      }
    }
    val trainSongList = trainRating.map { x => (x._2, 1) }.groupBy(_._1).map { x => (x._1, 1) }

    // read the IDF from hive table: songid - idf
    val idfData = sc.textFile(songIDFPath).map { x => x.split('|') }.map { x => (x(0), x(1).toDouble) }.cache()
    val maxIDF = 1.0
    var song_idf = idfData.map { x => (x._1, x._2 / maxIDF) }.collect.toMap
    val B_songidf = sc.broadcast(song_idf)
    song_idf = null

    // get the mapping form song to special
    val specialSong = sc.textFile(specialSongPath).map { x => x.split('|') }.map { x => (x(0), x(1), x(2).toInt) }.groupBy(_._1).map { row =>
      val songid = row._2.toArray.map { x => (x._2, x._3) }.sortBy(_._2).take(100).map { x => x._1 }
      (row._1, songid)
    }.join(trainSongList).map { x => (x._1, x._2._1) }

    // get the mapping from special to song
    val similarSong = sc.textFile(similarSongPath).map { x => x.split('|') }.map { x => (x(0), x(1), x(2).toInt) }.groupBy(_._1).map { row =>
      val songid = row._2.toArray.map { x => (x._2, x._3) }.sortBy(_._2).take(100).map { x => x._1 }
      (row._1, songid)
    }.join(trainSongList).map { x => (x._1, x._2._1) }

    var song2song = specialSong.union(similarSong).groupBy(_._1).map { row =>
      val songid1 = row._1
      val songid2_list = row._2.toArray.map { x => x._2 }.flatten.distinct
      (songid1, songid2_list)
    }.collect.toMap
    val B_song2song = sc.broadcast(song2song)
    song2song = null

    // read the increment data from hive table: mid - songid - playdr
    val incrementRating = sc.textFile(incrementDataPath).map { x => x.split('|') }
      .map { x => (x(1), (x(0), x(2).toDouble)) }
      .join(trainSongList)
      .map { x => (x._2._1._1, x._1, x._2._1._2) }.map { x =>
      var playdur = x._3
      if (x._3 > 30) {
        playdur = 30
      }
      (x._1, x._2, playdur)
    }

    // read the played history data of each user from hive table: mid - songid
    val playHistoryData = sc.textFile(playHistoryPath).map { x => x.split('=') }.filter{x=>x.length==2}.map { x =>
      val user_id = x(0)
      val songlist = x(1).split(',')
      (user_id, songlist)
    }

    // read the recommended history data from hive table: mid - songid
    val recommendHistoryData = sc.textFile(recommendHistoryPath).map{ x => x.split('=') }.filter{x=>x.length==2}.map{ x =>
      val user_id = x(0)
      val songlist = x(1).split(',')
      (user_id, songlist)
    }

    // read the mapping from songid to singername from hive table: songid - singername
    val songidSingerData = sc.textFile(songidSingerPath).map { x => x.split('|') }.map { x => (x(0), x(1)) }.join(trainSongList).map { x =>
      (x._1, x._2._1)
    }

    // read the user content data from hive table: mid(userid) - userinfo
    val userInfoData = sc.textFile(userInfoPath).map { x => x.split('|') }.map { x =>
      val mid = x(0)
      val uf = x(1).split('=').map { s => s.toDouble }
      (mid, uf)
    }

    // reformat the user-song interaction into the form: (mid, songid_list, playdur_list, songid_to_be_filtered, userinfo)
    val newUserRows = incrementRating.groupBy(_._1).map { row =>
      val (indices, values) = row._2.map(e => (e._2, e._3)).unzip
      (row._1, (indices.toArray, values.toArray))
    }.leftOuterJoin(playHistoryData).map { x => (x._1, (x._2._1._1, x._2._1._2, x._2._2)) }
      .leftOuterJoin(recommendHistoryData).map { x => (x._1, x._2._1._1, x._2._1._2, x._2._1._3, x._2._2) }.map { x =>
      val played_songid = x._4
      val recommend_songid = x._5
      var songid_to_be_filtered = new Array[String](0)
      if (played_songid != None) {
        songid_to_be_filtered = songid_to_be_filtered.union(played_songid.toList(0))
      }
      if (recommend_songid != None) {
        songid_to_be_filtered = songid_to_be_filtered.union(recommend_songid.toList(0))
      }
      songid_to_be_filtered = songid_to_be_filtered.distinct
      (x._1, (x._2, x._3, songid_to_be_filtered))
    }.leftOuterJoin(userInfoData).map { x => (x._1, x._2._1._1, x._2._1._2, x._2._1._3, x._2._2) }


    // load product features and make it to a mapping
    val productFeatures = sc.textFile(productFeaturePath).map { x =>
      val songid_feature = x.split('=')
      val songid = songid_feature(0)
      val feature = songid_feature(1).split('|').map { x => x.toDouble }
      (songid, feature)
    }
    var pfmap = productFeatures.collect.toMap;
    val B_pfmap = sc.broadcast(pfmap)

    // mapping from songid to singername
    var ssmap = productFeatures.leftOuterJoin(songidSingerData).map { x =>
      var singername = "Unknown"
      if (x._2._2 != None) {
        singername = x._2._2.toList(0)
      }
      (x._1, singername)
    }.collect.toMap
    val B_ssmap = sc.broadcast(ssmap)
    ssmap = null

    // pre-compute some variables
    val songKey = pfmap.keys.toArray
    val songNum = songKey.length
    var P1 = new DoubleMatrix(rank, rank)
    for (i <- 0 to songNum - 1) {
      val tmpy = new DoubleMatrix(pfmap(songKey(i)))
      P1 = P1.add(tmpy.mmul(tmpy.transpose))
    }
    val B_P1 = sc.broadcast(P1)
    var P3 = DoubleMatrix.eye(rank).mmul(lambda)
    val B_P3 = sc.broadcast(P3)
    P1 = null
    P3 = null
    pfmap = null

    // computing the user Features (for new users)
    val newuf = newUserRows.map { line =>
      val mid = line._1
      val songid = line._2
      val playdur = line._3
      val uf = computeUserFeatures(songid, playdur, alpha, rank, B_P1.value, B_P3.value, B_pfmap.value, B_songidf.value)
      var userInfo = "NotEmpty"
      if (line._5 == None) {
        userInfo = "Empty"
      }
      (mid, uf, songid, playdur, line._4, userInfo)
    }

    // remove the bad songids and preserve the good songs
    val productFeatures_filtered = productFeatures.join(goodSongid).map { x => (x._1, x._2._1) }
      .leftOuterJoin(badSongid).filter { x => x._2._2 == None }.map { x => (x._1, x._2._1) }

    pfmap = productFeatures_filtered.collect.toMap
    val B_pfmap_filtered = sc.broadcast(pfmap)
    pfmap = null

    var (songid5w, productFactors5w) = top5wProductFeatures(productFeatures_filtered)
    val B_productFactors5w = sc.broadcast(productFactors5w)
    val B_songid5w = sc.broadcast(songid5w)
    productFactors5w = null
    songid5w = null

    // perform recommendation
    val newRating = newuf.map { line =>
      val mid = line._1
      val uf = line._2
      val songid = line._3
      val playdur = line._4
      val songid_to_be_filtered = line._5
      val userInfo = line._6
      val candidateSongid = get_candidate_songlist(songid, playdur, B_song2song.value)
      val (tag, songlist) = generate_recommendation_list(uf, userInfo, candidateSongid, songid_to_be_filtered, B_songid5w.value, B_productFactors5w.value, B_pfmap_filtered, B_ssmap)
      (mid, songlist, tag)
    }

    // save the recommendation lists for each user (some for output directly, and some for second sorting).
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(savePath), true)
    } catch {
      case _: Throwable => {}
    }
    newRating.map { x => x._1 + "=" + x._2.mkString(",") + "=" + x._3 }.saveAsTextFile(savePath)
  }


  def computeUserFeatures(songid: Array[String], playdur: Array[Double], alpha: Double, rank: Int, P1: DoubleMatrix, P3: DoubleMatrix, pfmap: Map[String, Array[Double]], song_idf: Map[String, Double]): DoubleMatrix = {
    val songNum = songid.length
    var uf = new DoubleMatrix(rank)
    if (songNum > 1000) {
      var P2 = new DoubleMatrix(rank, rank)
      var B = new DoubleMatrix(rank)
      for (i <- 0 to songNum - 1) {
        val tmpFeature = pfmap(songid(i))
        val tmpy = new DoubleMatrix(tmpFeature)
        val idf = song_idf(songid(i))
        val r = computeRating(playdur(i), idf)
        val c = 1 + alpha * r
        P2 = P2.add(tmpy.mmul(c - 1).mmul(tmpy.transpose))
        B = B.add(tmpy.mmul(c))
      }
      val A = P1.add(P2).add(P3.mmul(songNum))
      uf = Solve.solve(A, B)
    } else {
      val tmpFeature = new Array[Array[Double]](songNum)
      val rating = new DoubleMatrix(songNum)
      for (i <- 0 to songNum - 1) {
        tmpFeature(i) = pfmap(songid(i))
        val idf = song_idf(songid(i))
        val r = computeRating(playdur(i), idf)
        rating.put(i, r)
      }
      val tmpY = new DoubleMatrix(tmpFeature)
      val tmpC = rating.mmul(alpha).add(1.0)
      val tmpD = DoubleMatrix.diag(tmpC)
      val tmpI = DoubleMatrix.eye(songNum)
      val P2 = tmpY.transpose.mmul(tmpD.sub(tmpI)).mmul(tmpY)
      val A = P1.add(P2).add(P3.mmul(songNum))
      val B = tmpY.transpose.mmul(tmpC)
      uf = Solve.solve(A, B)
    }
    uf
  }


  def get_candidate_songlist(songid: Array[String], playdur: Array[Double], song2song: Map[String, Array[String]]): Array[String] = {
    var candidate_songid = new Array[String](0)
    val favorite_songid = songid.zip(playdur).sortBy(_._2).reverse.take(30).filter { x => x._2 >= 2.0 }.map { x => x._1 }
    if (favorite_songid.length > 0) {
      candidate_songid = favorite_songid.flatMap { x =>
        var tmp_list = new Array[String](0)
        if (song2song.contains(x)) {
          tmp_list = song2song(x)
        }
        tmp_list
      }
    }
    if (candidate_songid.length > 0) {
      candidate_songid = candidate_songid.distinct.take(5000)
    }
    candidate_songid
  }


  def generate_recommendation_list(uf: DoubleMatrix, userInfo: String, candidateSongid: Array[String], songid_to_be_filtered: Array[String], songid5w: Array[String], productFactors5w: DoubleMatrix, B_pfmap: Broadcast[Map[String, Array[Double]]], B_ssmap: Broadcast[Map[String, String]]): (Int, Array[String]) = {
    val recommendNum = 60
    var songlist = new Array[String](recommendNum + 100)
    var tag = 1
    if (userInfo == "Empty") {
      songlist = new Array[String](recommendNum)
      tag = 0
    }
    val candidateNum = candidateSongid.length
    val songid_feature = new Array[(String, Array[Double])](candidateNum)
    for (i <- 0 to candidateNum - 1) {
      val tmpSongid = candidateSongid(i)
      if (B_pfmap.value.contains(tmpSongid) && !songid_to_be_filtered.contains(tmpSongid)) {
        songid_feature(i) = (tmpSongid, B_pfmap.value(tmpSongid))
      }
    }
    val (s, f) = songid_feature.filter { x => x != null }.unzip
    var filteredSongid = songid5w
    var productFactors = productFactors5w
    if (s.length > 60) {
      filteredSongid = s.toArray
      productFactors = new DoubleMatrix(f.toArray)
    }
    val r = productFactors.mmul(uf)
    val index = r.sortingPermutation()
    val songNum = filteredSongid.length
    var singerCount: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.Map()
    var i = songNum - 1
    var j = 0
    while (j <= songlist.length - 1 && i >= 0) {
      val tmpSongid = filteredSongid(index(i))
      val singername = B_ssmap.value(tmpSongid)
      if (!songid_to_be_filtered.contains(tmpSongid)) {
        if (!singerCount.contains(singername)) {
          songlist(j) = tmpSongid
          j = j + 1
          singerCount += (singername -> 1)
        } else {
          if (singerCount(singername) < 3) {
            songlist(j) = tmpSongid
            j = j + 1
            singerCount(singername) += 1
          }
        }
      }
      i -= 1
    }
    (tag, songlist.filter { x => x != null })
  }


  def top5wProductFeatures(productFeatures: RDD[(String, Array[Double])]): (Array[String], DoubleMatrix) = {
    val productFeature_temp = productFeatures.map { x =>
      val pfVector = new DoubleMatrix(x._2)
      val pfNorm = pfVector.transpose().mmul(pfVector)
      (x._1, x._2, pfNorm.get(0))
    }

    val songNorm = productFeature_temp.map { x => x._3 }.collect
    val songNormVector = new DoubleMatrix(songNorm)
    val idx = songNormVector.sortingPermutation()
    val hotN = 50000
    val hotSongCutoff = songNormVector.get(idx(songNorm.length - hotN))

    val productFiltered = productFeature_temp.filter { x => x._3 >= hotSongCutoff }.map { x => (x._1, x._2) }

    val pfmap = productFiltered.collect.toMap
    val filteredSongid = pfmap.keys.toArray
    val productFactors = new DoubleMatrix(pfmap.values.toArray)
    (filteredSongid, productFactors)
  }


  def computeRating(playcnt: Double, idf: Double): Double = {
    val r = Math.log10(playcnt / 0.01 + 1) * idf
    r
  }
}
