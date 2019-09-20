import java.io.{BufferedWriter, File, FileWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.native.JsonParser._

object srinagapavanisirichandana_pisupati_task2 {

  class CustomPartitioner(num_partitions: Int) extends Partitioner {
    override def numPartitions: Int = num_partitions

    override def getPartition(key: Any): Int = {
      val code = key.hashCode % numPartitions
      if (code < 0) {code + numPartitions} else { code }
    }
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Usage: program <input_file_name> <output_file_name> n_partition")
      return ()
    }

    val input_file = args(0)

    val ofp = new File(args(1))
    if (!ofp.isFile && !ofp.createNewFile()){}

    val n_partition = Integer.parseInt(args(2))

    Logger.getLogger("org").setLevel(Level.INFO)

    val ss = SparkSession
      .builder()
      .appName("Task1")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val sc = ss.sparkContext
    val reviews = sc.textFile(input_file).repartition(2).map(parseReview)

    val common = reviews.map(bid => (bid,1)).reduceByKey(_+_)

    val topBusinessesDefault = common
      .sortBy({case (x,y) => (-1 * y, x)})

    val defaultInit = System.currentTimeMillis()
    topBusinessesDefault.take(10)
    val defaultDone = System.currentTimeMillis() - defaultInit

    val topBusinessesCustom = common.partitionBy(new CustomPartitioner(n_partition)).sortBy({case (x,y) => (-1 * y, x)})

    val customInit = System.currentTimeMillis()
    topBusinessesCustom.take(10)
    val customDone = System.currentTimeMillis() - customInit

    val explanation = if (defaultDone < customDone) {
      "The default partitioner with " + topBusinessesDefault.getNumPartitions +
        " partitions is performing better than our custom partitioner with " + n_partition + "partitions."
    } else {
      "The custom partitioner, hashing the keys, with " + n_partition +
        " partitions is performing better than the default partitioner with " +
        topBusinessesDefault.getNumPartitions + " partitions."
    }

    val output_file = new BufferedWriter(new FileWriter(ofp))
    output_file.write("{\"default\": ")
    output_file.write("{\"n_partition\": " + topBusinessesDefault.getNumPartitions + ",")
    output_file.write(" \"n_items\": " + "[")
    printArr(output_file, topBusinessesDefault.glom().map(_.length).collect())
    output_file.write("]" + " \"exe_time\": " + defaultDone + "},")
    output_file.write(" \"customized\": ")
    output_file.write("{\"n_partition\": " + topBusinessesCustom.getNumPartitions + ",")
    output_file.write(" \"n_items\": " + "[")
    printArr(output_file, topBusinessesCustom.glom().map(_.length).collect())
    output_file.write("]" + " \"exe_time\": " + customDone + "},")
    output_file.write(" \"explanation\": " + "\"" + explanation +  "\"}")
    output_file.close()

  }

  def parseReview(r: String): String = {
    implicit val formats = DefaultFormats
    //    ((parse(r))\"date").extract[String].take(4)=="2018"
    parse(r, parser)
  }
  val parser = (p: Parser) => {
    def parse: String = p.nextToken match {
      case FieldStart("business_id") => p.nextToken match {
        case StringVal(bid) => bid
        case _ => p.fail("expected int")
      }
      case End => p.fail("no field named 'postalCode'")
      case _ => parse
    }

    parse
  }
  def printArr(op: BufferedWriter, top_users: Array[Int]): Unit = {
    val l = top_users.length
    var j = 0
    for (i <- top_users) {
      j = j+1
      if (j == l) {
        op.write(i.toString)
      } else {
        op.write(i+", ")
      }
    }
  }
}