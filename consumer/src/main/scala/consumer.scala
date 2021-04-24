import com.google.inject.matcher.Matchers.any
import org.apache.kafka.clients.consumer.{ConsumerRecord,ConsumerConfig, KafkaConsumer}

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



object consumer extends Serializable {

  
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    // initialize stream (same than producer)
    props.put("bootstrap.servers","localhost:9092")
    //define ID of the consumer --> name : kafka-example
    props.put(ConsumerConfig.GROUP_ID_CONFIG,"kafka-example")
    props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset","latest")

    // receip variable types
    val consumer : KafkaConsumer[String,String] = new KafkaConsumer[String,String](props)
    // example-topic needed, same topic name for producer & consumer
    consumer.subscribe(util.Arrays.asList("example-topic"))

    antiboucle(consumer)
  }

  def antiboucle(consumer: KafkaConsumer[String,String]):Unit={
    // receip record that producer sent, check every second
    val record = consumer.poll(1000).asScala

    // will take the record that is a long string, and will split with each comma
    val sentence = record.map(_.value()).flatMap(_.split(","))
    // same thing with ':'
    val sentence2 = sentence.flatMap(_.split(":"))
    // recreate a list
    val something = sentence2.toList

    // remove each specific character 
    val newsom = something.map(x => x.replace("[","")
      .replace("]","")
      .replace("{","")
      .replace("}","")
      .replace(",","")
      .replace(" ","")
    )
    // show time
    println(LocalDateTime.now())

    //if report
    if(newsom.nonEmpty){
      println(newsom)
      //take 5th value of the report (number of citizen in the report)
      val x = newsom(5).toInt
      //from place 11 to 11+x: citizen names
      val citizenname = newsom.slice(11,11+x)
      //from 11+x+1 to 11+2x+1: citizen score
      val citizenscore = newsom.slice(11+x+1,11+x+1+x)
      //takes score below 25
      val test = citizenscore.zipWithIndex.filter(x => (x._1).toInt < 25).map( x => (x._2))
      //if there is citizen with a score below 25, print alert with name & score of this citizen
      if(test.nonEmpty){
        test.foreach(
          x => {
            println("ALERT ! Citizen:"+citizenname(x)+"has a score of :"+citizenscore(x))
          }
        )

      }

    }
    //resend stream to the function
    antiboucle(consumer)
  }


}


