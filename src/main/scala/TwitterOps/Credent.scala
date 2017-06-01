package TwitterOps

/**
  * Created by cxa123230 on 1/31/2017.
  */
object Credent {
  // Configure Twitter credentials
  val apiKey = "5Ig8fR5gHbAcQ8CT3HDBg"
  val apiSecret = "PYKiuziQ5tO2xKZ84H153gZC1al0uvXgis7mHEKPd0"
  val accessToken = "38813037-BS6frsDmK0jXyteZqIy1TpeLQiUQgyOarJmyMixFY"
  val accessTokenSecret = "HvRAgoTxQmHGNTZdbvrwKzhcwkaRvIJ192TEnRAjnw"
  val credArr = scala.collection.mutable.ArrayBuffer.empty[Credentials]
  credArr.append(Credentials("5Ig8fR5gHbAcQ8CT3HDBg", "PYKiuziQ5tO2xKZ84H153gZC1al0uvXgis7mHEKPd0", "38813037-BS6frsDmK0jXyteZqIy1TpeLQiUQgyOarJmyMixFY", "HvRAgoTxQmHGNTZdbvrwKzhcwkaRvIJ192TEnRAjnw"))
  credArr.append(Credentials("hIYPR9JUGC62jfDooTSHig", "886JW8PrT7wKs6DYIynRtWsCk44OOYQQusBqQ03C8", "38813037-cf3RQ3BUlQRQDAOVvZ5GprlhdR9rT8IrEaPC5zg", "s2czFpUOW8afCXR8SlO6GKY0iHHsL9woA6TQwLoICg"))

  def getApp(ind: Int): Credentials = {
    return credArr(ind)
  }
}

case class Credentials(val apiKey: String, val apiSecret: String, val accessToken: String, val accessTokenSecret: String)