import java.io.FileWriter

import org.apache.commons.httpclient.util.URIUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.parsing.json.JSON


/**
  * Created by cxa123230 on 1/23/2017.
  * Experiment 2. Requires results from Holder.scala experiments
  */
object LocationHandler {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
  val outFile = new FileWriter("C:/Users/cxa123230/Dropbox/Publications/PostDoc work/GraphBoot/validation data/2015GB.txt")
  val predFile: String = "results/classified/1resultPredsRestrictive.txt"
  val locFile: String = "results/data/existingLocs.txt"
  val abbFile: String = "C:/Users/cxa123230/Dropbox/Publications/PostDoc work/GraphBoot/validation data/stateAbb.txt"
  private val apiFile: String = "results/data/apikey.txt"


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("graphboot")
      .master("local[16]")
      .getOrCreate()
    val sc = spark.sparkContext

    val key = Source.fromFile(apiFile).getLines().toList(0).toString
    val users = Source.fromFile(predFile).getLines()
    val allusers: List[Array[String]] = users.map(e => e.split("\t")).toList
    val illUsers: Set[String] = allusers.filter(e => e(1) != "0.0").map(e => e(0)).toSet
    val wellUsers: Set[String] = allusers.filter(e => e(1).toDouble == 0.0).map(e => e(0)).toSet

    println(illUsers.size + " users were predicted to have a depression label. " + wellUsers.size + " have not.")

    val allUsersAndLocations: List[(String, String)] = Source.fromFile(ClassifierData.infoFile).getLines().map(e => e.split("\t")).map(e => (e(1), e(2))).toList
    println("we have discovered information on " + allUsersAndLocations.size + " users.")
    val illUsersAndLocs: Map[String, String] = allUsersAndLocations.filter(e => illUsers.contains(e._1)).filter(e => e._2.length > 2).map(e => (e._1, e._2.toLowerCase)).toMap
    val wellUsersAndLocs: Map[String, String] = allUsersAndLocations.filter(e => wellUsers.contains(e._1)).filter(e => e._2.length > 2).map(e => (e._1, e._2.toLowerCase)).toMap
    println(wellUsersAndLocs.size + " well users' locations found.")
    println(illUsersAndLocs.size + " ill users' locations found.")
    val illLocs: Set[String] = illUsersAndLocs.map(e => e._2.toLowerCase()).toSet
    println(illLocs.size + " unique ill locations found.")
    val wellLocs: Set[String] = wellUsersAndLocs.map(e => e._2.toLowerCase()).toSet
    println(wellLocs.size + " unique well locations found.")


    val existing: Map[String, String] = findExistingLocations(locFile)
    val illLocs2beFound: Set[String] = illLocs.diff(existing.keySet)
    val wellLocs2beFound: Set[String] = wellLocs.diff(existing.keySet)
    println(illLocs2beFound.size + " unique ill locs to be found, already have " + existing.size)
    println(wellLocs2beFound.size + " unique well locs to be found.")
    val fetch = true
    val buf: ArrayBuffer[(String, String)] = if (fetch) findMissingLocations(locFile, wellLocs2beFound, key) else ArrayBuffer.empty[(String, String)]

    buf.appendAll(existing)
    val idMap = buf.toMap[String, String]
    val illMapVals: mutable.Map[String, Int] = getStateDistribution(illUsersAndLocs, idMap)
    val wellMapVals: mutable.Map[String, Int] = getStateDistribution(wellUsersAndLocs, idMap)
    printStateDistribution(illMapVals, wellMapVals)
  }

  def findExistingLocations(locFile: String): (Map[String, String]) = {

    val existing: Map[String, String] = Source.fromFile(locFile).getLines().map(e => ({
      val sp = e.toLowerCase().split("\t")
      (sp(0), sp(1))
    })).toMap
    (existing)
  }

  def findMissingLocations(locFile: String, locs2beFound: Set[String], key: String): ArrayBuffer[(String, String)] = {
    val locWriter = new FileWriter(locFile, true)
    val buf = mutable.ArrayBuffer.empty[(String, String)]
    for (row <- locs2beFound.take(2500)) {
      val address = row
      try {

        val country: String = getLoc(address, key)
        locWriter.append(address + "\t" + country + "\r\n")
        buf.append((address, country))
      } catch {
        case ioe: Exception => {

          //println(ioe.getLocalizedMessage)
          locWriter.append(address + "\t" + "invalid:exception raised" + "\r\n")
          buf.append((address, "invalid:exception raised"))
        }
      }
    }
    locWriter.close()
    buf
  }

  def getLoc(addr: String, key: String): String = {
    val invalid: String = "invalid"
    if (addr.length == 0) return invalid
    val address = URIUtil.encodeQuery(addr)

    val result = get("https://maps.googleapis.com/maps/api/geocode/json?address=" + address + "&sensor=false&key=" + key).mkString

    val parsed = JSON.parseFull(result)
    val m0: Map[String, List[Any]] = parsed.get.asInstanceOf[Map[String, List[Any]]]
    if (m0("status") == "ZERO_RESULTS") return invalid

    val m1: Any = m0("results")(0)
    val globalMap = m1.asInstanceOf[Map[String, Any]]
    val country = globalMap.get("formatted_address").get.asInstanceOf[String]
    country
  }

  def get(url: String) = scala.io.Source.fromURL(url)
  def getStateDistribution(usersAndLocs: Map[String, String], idMap: Map[String, String]): mutable.Map[String, Int] = {
    val abs: Map[String, String] = Source.fromFile(abbFile).getLines().map(e => (e.split("\t")(1).toLowerCase(), e.split("\t")(0).toLowerCase())).toMap
    val mapVals: mutable.Map[String, Int] = mutable.HashMap.empty[String, Int].withDefaultValue(0)
    for (row <- usersAndLocs) {
      val usr = row._1
      if (idMap.contains(row._2)) {
        val add = idMap(row._2)
        if (add.contains(", usa")) {
          val a = add.split(",")
          val a2 = a(a.length - 2).replaceAll("[0-9]+", "").trim
          if (abs.contains(a2)) mapVals(abs(a2)) += 1
          else mapVals(a2.toLowerCase()) += 1
        }
      }
    }

    mapVals
  }

  def printStateDistribution(illStateMap: mutable.Map[String, Int], wellStateMap: mutable.Map[String, Int]): Unit = {

    outFile.append("region\tor18\twell\r\n")
    for (v <- illStateMap.keySet.union(wellStateMap.keySet)) {
      val loc = v
      val cou = if (illStateMap.contains(loc)) illStateMap(loc) else 0
      val dc = if (wellStateMap.contains(loc)) wellStateMap(loc) else 0
      outFile.append(loc + "\t" + cou + "\t" + dc + "\r\n")
    }

    if (!wellStateMap.contains("idaho"))
      outFile.append("idaho\t0\t0\r\n")
    if (!wellStateMap.contains("south dakota"))
      outFile.append("south dakota\t0\t0\r\n")
    outFile.close()
  }

}
