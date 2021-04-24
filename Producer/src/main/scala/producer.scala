

import model.Report
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import play.api.libs.json._

import java.io.{File, PrintWriter}
import java.util.Properties
import java.time.temporal.ChronoUnit
import java.time.LocalDate
import java.time.Month
import java.util.concurrent.ThreadLocalRandom

object producer {

// Initialization of our variables
  val pw = new PrintWriter(new File("Report.txt"))
  val names = List("Jhon", "Jerry", "Mickael", "Mary", "Jane", "Patrick", "Laura", "Daniel", "Edith", "Rihanna")
  val word = List("good", "bad", "police", "candle", "fire", "ok", "yes", "safe", "crime", "urgency", "fireworks", "pineapple", "turtle", "water", "pencil", "glasses", "afraid", "ring", "threat", "thanks", "happy", "snow")
  val entierscore = 0
  val listname = 0
  val listWord = 0

// Generate the score of each citizen 
  def Score(i: Int): List[Int] = {
    //generate an integer list between [0-100], 1 for each name 
    val entierscore = List.fill(i)(100).map(_ => scala.util.Random.nextInt(100))
    //avoid return
    if (i > 0) {

      entierscore
    }
    else {
      entierscore
    }

  }

// Generate each citizen name
  def Citizen(i: Int): List[String] = {
    // give to each person a name existing in the name's list above
    val listname = List.fill(i)(names(scala.util.Random.nextInt(names.length)))
    if (i >= 0) {
      listname
    }
    else {
      listname
    }
  }

// Generate i words listened by the peacewatcher
  def WordHeard(i: Int): List[String] = {
    val listWord = List.fill(i)(word(scala.util.Random.nextInt(word.length)))
    if (i >= 0) {
      listWord
    }
    else {
      listWord
    }

  }

// create report
  def report(i: Int): Unit = {
    //condition to avoid infinite report sending
    if (i > 0) {
      //number of citizen seen by peacewatcher (limited to 5 here)
      val number = scala.util.Random.nextInt(5)
      //number of words listened by peacewatcher (limited to 22 here)
      val numberword = scala.util.Random.nextInt(22)

      val citizenname = Citizen(number)
      val citizenscore =Score(number)
      //generate random ID for each peacewatcher
      val ID = scala.util.Random.nextInt(1000)
      //generate float for place where peachwatcher is
      val Longitude = scala.util.Random.nextFloat()
      val Latitude = scala.util.Random.nextFloat()

      val heard = WordHeard(numberword)
      // generate date for report (max/min)
      val start = LocalDate.of(2021,Month.JANUARY,1)
      val stop = LocalDate.of(2022,Month.JANUARY,1)
      // Compute max number of day this year
      val maxDays = ChronoUnit.DAYS.between(start,stop)
      // generate date
      val days = ThreadLocalRandom.current.nextLong(0, maxDays+1)
      val date = start.plusDays(days)
      // Test report
      pw.write("Date"+date+"ID DRONE :" + ID +",Population:"+ number +", LOCALISATION [" + Longitude + ";" + Latitude + "]," + "CitizenName:" + citizenname + ", CitizenScore:"+ citizenscore + ",Mot:" +heard + "\n")

      // case class containing each variables for senting in 1 time
      val reportdrone = Report(date,ID,number, Longitude, Latitude, citizenname,citizenscore, heard)
      val reportjson = Json.toJson(reportdrone)

      // initialize stream
      val props = new Properties()
      props.put("bootstrap.servers","localhost:9092")
      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String,String](props)
      val record = new ProducerRecord[String,String]("example-topic","key",Json.stringify(reportjson))

      // display state
      println("send Record")
      // send record into stream
      producer.send(record)
      producer.close()



      producer.close()

      report(i - 1)
    }
  }


  def main(args: Array[String]): Unit = {
    //send 1 report, when finish, wait 30s restart (recursive main)
    report(1)
    pw.close()
    Thread.sleep(30000)
    main(args)
  }


}

