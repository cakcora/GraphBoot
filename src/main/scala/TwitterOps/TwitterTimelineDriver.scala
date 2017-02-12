package TwitterOps

/**
  * Created by cxa123230 on 1/31/2017.
  */

import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import twitter4j._
import twitter4j.conf.ConfigurationBuilder

import scala.collection.mutable
import scala.io.Source

object TwitterTimelineDriver {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)


  def main(args: Array[String]) {

    val cb = new ConfigurationBuilder()
    val cre = Credent.getApp(1)
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(cre.apiKey)
      .setOAuthConsumerSecret(cre.apiSecret)
      .setOAuthAccessToken(cre.accessToken)
      .setOAuthAccessTokenSecret(cre.accessTokenSecret)
    val tf = new TwitterFactory(cb.build())
    val twitter = tf.getInstance()


    val filename: String = "userinfo.txt"
    val file2name = "timeline.txt"
    val seeds: Set[String] = Source.fromFile("seedList.txt").getLines().toSet
    val nextList: Set[String] = getNext(filename, file2name, seeds, wave = 1)
    var count = 0;
    val rate = 900
    val window: Int = 15
    val fw = new FileWriter(file2name, true)
    var nextTime = System.currentTimeMillis() + window * 60 * 1000
    var sofar = 0
    println("will download " + nextList.size)
    for (user <- nextList) {
      count = (count + 1) % (rate + 1)
      if (count == 0) {
        while (nextTime > System.currentTimeMillis()) {
          val waitTime: Long = 10 * 1000
          Thread.sleep(waitTime)
        }
        nextTime = System.currentTimeMillis() + window * 60 * 1000
        count = 1
      }
      sofar += 1
      //if(sofar%450==0)
      try {
        val se: mutable.Buffer[Status] = getTimeLine(twitter, user)
        se.foreach(e => fw.append(e.toString + "\r\n"))
        fw.flush()
        println("Using resource " + count + " for " + user + " at time " + System.currentTimeMillis() / 1000)
      } catch {
        case e: Exception => println(user + " " + e.getLocalizedMessage)
      } finally {}

    }
    fw.close()
  }


  def getTimeLine(twitter: Twitter, user: String): mutable.Buffer[Status] = {
    val se = scala.collection.mutable.Buffer.empty[Status]
    val f = twitter.getUserTimeline(user).iterator()
    val regex: String = "\\p{C}|\\s+|\\r$|\\\\t|\\\\n|\\\\r"
    while (f.hasNext()) {
      val s = f.next()
      val con = if (s.getContributors != null) s.getContributors.mkString(" ") else " "
      val cre = s.getCreatedAt.toString
      val cur = s.getCurrentUserRetweetId
      val fav = s.getFavoriteCount
      val geo = s.getGeoLocation
      val in1 = s.getInReplyToScreenName
      val in2 = s.getInReplyToStatusId
      val in3 = s.getInReplyToUserId
      val lan = s.getLang
      val pla = s.getPlace
      val ret = s.getRetweetCount
      val tex = s.getText.replaceAll(regex, " ")
      val pos = s.isPossiblySensitive
      val tru = s.isTruncated
      val sta = Status(user, con, cre, cur, fav, geo, in1, in2, in3, lan, pla, ret, tex, pos, tru)
      se += sta
    }
    return se
  }

  def getNext(filename: String, file2name: String, seeds: Set[String], wave: Int): Set[String] = {
    val f1 = Source.fromFile(filename).getLines().toArray.filter(e => seeds.contains(e.split("\t")(0))).map(e => e.split("\t")(1)).toSet


    if (wave < 2) {

      return getNext(filename, file2name, f1, wave + 1)
    }
    else {
      val f2 = Source.fromFile(file2name).getLines().toArray.map(e => e.split("\t")(0)).toSet

      val tobeFound = f1.diff(f2)
      //      println(wave+", "+tobeFound.size+" are found")
      return tobeFound
    }
  }
}


case class Status(user: String, con: String, cre: String, cur: Long, fav: Int, geo: GeoLocation, in1: String, in2: Long, in3: Long, lan: String, pla: Place, ret: Int, tex: String, pos: Boolean, tru: Boolean) {
  override def toString = s"$user\t$con\t$cre\t$cur\t$fav\t$geo\t$in1\t$in2\t$in3\t$lan\t$pla\t$ret\t$tex\t$pos\t$tru"
}


