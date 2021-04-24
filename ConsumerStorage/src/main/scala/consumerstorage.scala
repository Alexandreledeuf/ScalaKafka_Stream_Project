import com.google.inject.matcher.Matchers.any
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer,ConsumerConfig}

import scala.collection.JavaConverters._
import java.util.Properties
import java.util
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import spray.json._
import DefaultJsonProtocol._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import java.time.LocalDateTime
import java.io._



object consumerstorage extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  val spark = SparkSession.builder().master("local[2]").appName("Spark Storage").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    //initialize stream
    val props = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    //different id than consumer1
    props.put(ConsumerConfig.GROUP_ID_CONFIG,"kafka-example-2")
    props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset","latest")

    //receip variable types
    val consumer : KafkaConsumer[String,String] = new KafkaConsumer[String,String](props)
    consumer.subscribe(util.Arrays.asList("example-topic"))
    antiboucle(consumer)


  }



  def antiboucle(consumer: KafkaConsumer[String,String]):Unit={
    val record = consumer.poll(1000).asScala
    //verify that report.csv & reportAlert.csv exist
    if(scala.reflect.io.File("report.csv").exists && scala.reflect.io.File("reportAlert.csv").exists  ){
      val sentence = record.map(_.value()).flatMap(_.split(","))
      val sentence2 = sentence.flatMap(_.split(":"))
      val something = sentence2.toList
      val newsom = something.map(x => x.replace("[","")
        .replace("]","")
        .replace("{","")
        .replace("}","")
        .replace(",","")
        .replace(" ","")
      )
      println(LocalDateTime.now())
      
      //same thing than in consumer.scala, but with a file writing method
      //write in report file
      if(newsom.nonEmpty){
        println(newsom)
        println("Write to CSV File")
        val x = newsom(5).toInt
        val citizenname = newsom.slice(11,11+x)
        val citizenscore = newsom.slice(11+x+1,11+x+1+x)
        val wordheard = newsom.slice(11+2*x+1,(newsom.length)-1)
        val writeFile = new File("report.csv")
        val input = (newsom(1)+";"+newsom(3)+";"+newsom(5)+";"+newsom(7)+";"+newsom(9)+";"+citizenname+";"+citizenscore+";"+wordheard+"\n")
        val writer = new BufferedWriter(new FileWriter(writeFile,true))
        writer.write(input)
        writer.close()
        val test = citizenscore.zipWithIndex.filter(x => (x._1).toInt < 25).map( x => (x._2))

        //if yes, enter in loop and write
        //write in alert file
        if(test.nonEmpty){
          test.foreach(
            x => {
              println("ALERT ! Citizen:")
              val writeFileAlert = new File("reportAlert.csv")
              val inputAlert = (newsom(1)+";"+newsom(3)+";"+newsom(5)+";"+newsom(7)+";"+newsom(9)+";"+citizenname(x)+";"+citizenscore(x)+";"+wordheard+"\n")
              val writerAlert = new BufferedWriter(new FileWriter(writeFileAlert,true))
              writerAlert.write(inputAlert)
              writerAlert.close()
            }
          )
        }
      }
    }
    //if files doesn't exist, create them and instance the first row (column names)
    else{
      println("Create File")
      val writeFile = new File("report.csv")
      val writeFileAlert = new File("reportAlert.csv")
      val input = ("Date;ID_Drone;Population;Longitude;Latitude;CitizenName;CitizenScore;Word\n")
      val writer = new BufferedWriter(new FileWriter(writeFile,false))
      val writerAlert = new BufferedWriter(new FileWriter(writeFileAlert,false))
      writer.write(input)
      writerAlert.write(input)
      writer.close()
      writerAlert.close()
    }
    antiboucle(consumer)
  }


}


