import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, to_date}
import org.apache.log4j.Logger
import org.apache.log4j.Level



object Analyser{
  def main(args: Array[String]): Unit = {
    //remove useless log info from terminal 
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("info").setLevel(Level.OFF)

    //initialize spark
    val spark = SparkSession.builder().master("local[1]").appName("Spark").getOrCreate()
    import spark.implicits._
    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
    println("Load csv... ")
    //create dataframe with the report csv
    val df = sqlContext.read.format("com.databricks.spark.csv").options(Map("inferSchema"->"true","delimiter"->";","header"->"true")).load("/home/alexandre/Bureau/consumerstorage/reportAlert.csv")
    //modify the form of date to have the good form for sql
    val df2 = df.withColumn("month",date_format(to_date($"date","yyyy-MM-dd"),"MMMM"))
    //create name of the dataframe to do request on it
    df2.createOrReplaceTempView("Dataframe")
    println("Load query... ")

    //First SQL Query Find the total of alert
    val query0 = spark.sql("""SELECT count(Date) as totalalert FROM Dataframe """)

    //second SQL Query Find the amount of alert per month
    val query1 = spark.sql("""SELECT month,count(month) FROM Dataframe GROUP BY month""")

    //Third SQL Query Find the Citizen that got the lowest score and his location
    val query2 = spark.sql("""SELECT CitizenName,CitizenScore,Longitude,Latitude from Dataframe Where CitizenScore = (select min(CitizenScore) FROM Dataframe)""")

    //Fourth SQL Query Find the Citizen that got the higher score and his location
    val query3 = spark.sql("""SELECT CitizenName,CitizenScore,Longitude,Latitude from Dataframe Where CitizenScore = (select max(CitizenScore) FROM Dataframe)""")

    // display queries
    println("Display Dataframe... ")
    df2.show()
    println("First query... ")
    println("How many alerts ?")
    query0.show()
    println("Second query... ")
    println("How many alerts per month ?")
    query1.show()
    println("Third query... ")
    println("Which citizen got the lowest score ? What's his location ?")
    query2.show()
    println("Fourth query... ")
    println("Which citizen got the highest score ? What's his location ?")
    query3.show()
    println("End...")







  }

}
