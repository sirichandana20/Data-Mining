import java.io.{BufferedWriter, File, FileWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.native.JsonParser._

case class Rbs(business_id: String, stars: Float)
case class Bbc(business_id: String, city: String)

object srinagapavanisirichandana_pisupati_task3 {
  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      println("Usage: program <input_file_name1> <input_file_name2> <output_file_name1> <output_file_name2>")
      return ()
    }

    Logger.getLogger("org").setLevel(Level.INFO)

    val input_file_review = args(0)
    val input_file_business = args(1)

    val ofp = new File(args(2))
    if (!ofp.isFile && !ofp.createNewFile()){}

    val oft = new File(args(3))
    if (!oft.isFile && !oft.createNewFile()){}

    val ss = SparkSession
      .builder()
      .appName("Task3")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val sc = ss.sparkContext
    val parsed_reviews = sc.textFile(input_file_review).map(parseReview).map(x => (x.business_id, x.stars))
    val parsed_businesses = sc.textFile(input_file_business).map(parseBusiness).map(x => (x.business_id, x.city))

    val city_stars = parsed_businesses.leftOuterJoin(parsed_reviews).map(x => x._2)
      .filter({case (x,Some(y)) => true; case _ => false}).map({case (x,Some(y)) => (x,y)})
      .mapValues(v => (v,1)).reduceByKey({case ((x,acc1),(y,acc2)) => (x+y, acc1+acc2)})
      .mapValues(x => x._1 / x._2)
      .sortBy({case (x,y) => (-1 * y, x.toLowerCase())}, numPartitions = 1)
      .map(x => x._1 + ',' + Math.round(x._2 * 10) / 10.0)
      .persist(StorageLevels.MEMORY_AND_DISK)

    val res = city_stars.collect()

    val collect_init_seconds = System.currentTimeMillis()
    println("city,stars")
    val collect_stars = city_stars.collect().take(10)
    for (r <- collect_stars) {
      println(r)
    }
    val m1 = System.currentTimeMillis() - collect_init_seconds

    val take_init_seconds = System.currentTimeMillis()
    println("city,stars")
    val take_stars = city_stars.take(10)
    for (r <- take_stars) {
      println(r)
    }
    val m2 = System.currentTimeMillis() - take_init_seconds

//    val res = Array("x,1","y,2","z,3")
    val output_file_stars = new BufferedWriter(new FileWriter(ofp))
    output_file_stars.write("city,stars\n")
    printArr(output_file_stars, res)
    output_file_stars.close()

    val output_file_time = new BufferedWriter(new FileWriter(oft))
    output_file_time.write("{\"m1\": "+ m1 + ",")
    output_file_time.write(" \"m2\": "+ m2 + ",")
    val explanation = if (m1 < m2) {
      "Method 1 i.e.collecting all the data and printing the first 10 cities is faster than Method 2."
    } else {
      "Method 2 i.e. taking the first 10 cities is faster than Method 1, as only the the first 10 elements are collected into the array."
    }
    output_file_time.write(" \"explanation\": "+ explanation + "}")
    output_file_time.close()
  }

  def parseReview(r: String): Rbs = {
    implicit val formats = DefaultFormats
    //    ((parse(r))\"date").extract[String].take(4)=="2018"
    parse(r).extract[Rbs]
  }

  def parseBusiness(r: String): Bbc = {

    implicit val formats = DefaultFormats
    //    ((parse(r))\"date").extract[String].take(4)=="2018"
    parse(r).extract[Bbc]
  }

  def printArr(op: BufferedWriter, top_users: Array[String]): Unit = {
    val l = top_users.length
    var j = 0
    for (i <- top_users) {
      j = j+1
      if (j == l) {
        op.write(i)
      } else {
        op.write(i+"\n")
      }
    }
  }

}