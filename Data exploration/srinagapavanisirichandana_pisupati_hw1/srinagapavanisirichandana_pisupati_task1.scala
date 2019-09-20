import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Paths

import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.native.JsonMethods._

case class Review(user_id: String, business_id: String, date: String)

object srinagapavanisirichandana_pisupati_task1 {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: program <input_file_name> <output_file_name>")
      return ()
    }

    val input_file = args(0)

    val fp = new File(args(1))

    if (!fp.isFile && !fp.createNewFile()) {
    }

    Logger.getLogger("org").setLevel(Level.INFO)

    val ss = SparkSession
      .builder()
      .appName("Task1")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val sc = ss.sparkContext
    val reviews = sc.textFile(input_file).repartition(2).map(parseReview).
      persist(StorageLevels.MEMORY_AND_DISK)
    val review_counts_2018 = reviews.filter(r => r.date.take(4) == "2018")
    val distinct_users = reviews.map(_.user_id).distinct()
    val top_users = reviews.map(r => (r.user_id, 1)).reduceByKey(_ + _)
      .sortBy({case (x,y) => (-1 * y, x)}, numPartitions = 1).take(10)
    val distinct_businesses = reviews.map(_.business_id).distinct()
    val top_businesses = reviews.map(r => (r.business_id,1)).reduceByKey(_+_)
        .sortBy({case (x,y) => (-1 * y, x)}, numPartitions = 1).take(10)


    val output_file = new BufferedWriter(new FileWriter(fp))
    output_file.write("{\"n_review\": "+reviews.count() + ",")
    output_file.write(" \"n_review_2018\": "+review_counts_2018.count()+",")
    output_file.write(" \"n_user\" :"+distinct_users.count() + ",")
    output_file.write(" \"top10_user\": "+"[" + arr2String(top_users) + "],")
    output_file.write(" \"n_businesss\": "+distinct_businesses.count() + ",")
    output_file.write(" \"top10_business\": "+"["+arr2String(top_businesses)+"]}")
    output_file.close()


//    val json = ("n_review" -> 300) ~ ("n_review_2018" -> 32) ~ ("n_user" -> 43) ~ ("top10_user" -> List(("u1",32),("u2",65))) ~ ("n_business" -> 100)
  }

  def parseReview(r: String): Review = {
    implicit val formats = DefaultFormats
    //    ((parse(r))\"date").extract[String].take(4)=="2018"
    parse(r).extract[Review]
  }

  def arr2String(top_users: Array[(String, Int)]): String = {
    var result = ""
    val l = top_users.length
    var j = 0
    for (i <- top_users) {
      j = j+1
      if (j == l) {
        result = result + "[\"" + i._1 + "\", " +  i._2 + "]"
      } else {
        result = result + "[\"" + i._1 + "\", " +  i._2 + "]" + ", "
      }

    }
    return result
  }

}