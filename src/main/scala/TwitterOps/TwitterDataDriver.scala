package TwitterOps

/**
  * Created by cxa123230 on 1/31/2017.
  */

import java.io.FileWriter
import java.util
import java.util.Date

import org.apache.log4j.{Level, Logger}
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{Twitter, TwitterFactory, User}

import scala.collection.mutable
import scala.io.Source

object TwitterDataDriver {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)




  def main(args: Array[String]) {

    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(Credent.apiKey)
      .setOAuthConsumerSecret(Credent.apiSecret)
      .setOAuthAccessToken(Credent.accessToken)
      .setOAuthAccessTokenSecret(Credent.accessTokenSecret)
    val tf = new TwitterFactory(cb.build())
    val twitter = tf.getInstance()

    val seeds = List("CDepression_UK", "AlexElk123", "ImNotFine_x", "ChildMindDotOrg", "BipolarTu", "worthlivingnow", "natasha_tracy", "CPMHealthCare", "BipolarGrrl", "sadinthehead", "PROJECT375", "angelsdemonsorg", "m_vanackeren")

    val filename: String = "userinfo.txt"
    val nextList: Set[String] = getNextSet(filename)
    val fw = new FileWriter(filename, true)
    var count = 0;
    val rate = 15
    var nextTime = System.currentTimeMillis() + rate * 60 * 1000
    var remains = nextList.size
    for (seed <- nextList) {
      count = (count + 1) % (rate + 1)
      if (count == 0) {
        while (nextTime > System.currentTimeMillis()) {
          val waitTime: Long = 10 * 1000
          Thread.sleep(waitTime)
        }
        nextTime = System.currentTimeMillis() + rate * 60 * 1000
        count = 1
      }
      remains -= 1
      println(remains + " to go, using " + count + " for " + seed + " at time " + System.currentTimeMillis() / 1000)

      val cursor = -1
      try {
        val se: mutable.Buffer[TwitterUser] = getFolList(twitter, seed, cursor)
        se.foreach(e => fw.append(e.toString + "\r\n"))
        fw.flush()
      } catch {
        case e: Exception => println(e.getMessage)
      } finally {}

    }
    fw.close()
  }


  def getFolList(twitter: Twitter, seed: String, cursor: Int): mutable.Buffer[TwitterUser] = {
    val se = scala.collection.mutable.Buffer.empty[TwitterUser]
    val f = twitter.getFollowersList(seed, cursor, 200)
    val followers: util.Iterator[User] = f.iterator()
    val regex: String = "\\p{C}|\\s+|\\r$|\\\\t|\\\\n|\\\\r"
    while (followers.hasNext) {
      val f = followers.next
      val fro = seed
      val scr = f.getScreenName
      val loc = f.getLocation.replaceAll(regex, " ")
      val fol = f.getFollowersCount
      val fri = f.getFriendsCount
      val cre = f.getCreatedAt

      val des = f.getDescription.replaceAll(regex, " ")
      val lan = f.getLang
      val lis = f.getListedCount
      val wit = if (f.getWithheldInCountries == null) " " else f.getWithheldInCountries.mkString(",")
      val tim = f.getTimeZone
      val bac = f.getProfileBackgroundColor
      val sta = f.getStatusesCount
      val usr = TwitterUser(fro, scr, loc, fol, fri, cre, des, lan, lis, wit, tim, bac, sta)
      se += usr
    }
    println(" " + se.length + " followers found")
    return se
  }

  def getNextSet(fileName: String): Set[String] = {
    val f = Source.fromFile(fileName).getLines().toArray
    val tobeFound = f.map(e => e.split("\t")(1)).toSet.diff(f.map(e => e.split("\t")(0)).toSet)
    tobeFound
  }
}


case class TwitterUser(val fro: String, val scr: String, val loc: String, val fol: Int, val fri: Int, val cre: Date, val des: String, val lan: String, val lis: Int, val wit: String, val tim: String, val bac: String, val sta: Int) {


  override def toString = s"$fro\t$scr\t$loc\t$fol\t$fri\t$cre\t$des\t$lan\t$lis\t$wit\t$tim\t$bac\t$sta"

  override def equals(other: Any): Boolean = other match {
    case that: TwitterUser =>
      (that canEqual this) &&
        scr == that.scr &&
        cre == that.cre
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[TwitterUser]

  override def hashCode(): Int = {
    val state = Seq(scr, cre)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}


