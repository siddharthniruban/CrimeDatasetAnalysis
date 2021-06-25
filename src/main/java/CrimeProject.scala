import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{avg, col, desc, stddev_pop, to_timestamp}
import org.apache.spark.sql.{SQLContext, SparkSession}
import java.io.{BufferedWriter, File, FileWriter}
import org.apache.spark.sql.{DataFrame}
object CrimeProject {

  var spark:SparkSession  = SparkSession.builder().appName("Crime_Data").enableHiveSupport().
    config("hive.exec.dynamic.partition","true").config("hive.exec.dynamic.partition.mode","true").getOrCreate()
  var sc : SparkContext = spark.sparkContext
  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  var comm = new CommonUtil()

  def main(args : Array[String]): Unit ={

    var inputPath = args(0)
    var outputPath = args(1)
    var crimeDF = spark.read.option("header", "true").csv(inputPath)
    crimeDF.withColumn("Dates_new",to_timestamp(col("Dates"),"yyyy-MM-dd HH:mm:ss")).registerTempTable("crime_table")
    import sqlContext.implicits._

    //1. Amount of crime over years,months,hours
    val crime_month = spark.sql("select month(dates_new) as month_of_crime,count(*) from crime_table group by month(dates_new) order by count(*) desc")
    val crime_year = spark.sql("select year(dates_new) as year_of_crime,count(*) from crime_table group by year(dates_new) order by count(*) desc")
    val crime_hour = spark.sql("select hour(dates_new) as hour_of_crime,count(*) from crime_table group by hour(dates_new) order by count(*) desc")
    comm.writeToFileString("Amount of crime over years\nyear | Count of crime",outputPath)
    comm.DFwriteToFileIntLong(crime_year,outputPath)
    comm.writeToFileString("Amount of crime over months\nmonth | Count of crime",outputPath)
    comm.DFwriteToFileIntLong(crime_month,outputPath)
    comm.writeToFileString("Amount of crime over hours\nhour | Count of crime",outputPath)
    comm.DFwriteToFileIntLong(crime_hour,outputPath)

    var cols = crimeDF.columns
    comm.writeToFileString("Check the amount of unique values for each columns",outputPath)
    for(x <- cols){comm.writeToFileString("uniqueid in "+x+" : "+ crimeDF.select(x).distinct.count,outputPath)}

    comm.writeToFileString("Amount of crime per category\nCategory | Count of crime",outputPath)
    var crime_category = spark.sql("select Category,count(*) from crime_table group by category order by count(*) desc")
    comm.DFwriteToFileStringLong(crime_category,outputPath)
    comm.writeToFileString("Large Category is LARCENY/THEFT ,lets investigate in further",outputPath)
    comm.writeToFileString("For Category LARCENY/THEFT found crime over each DayofWeek\nDayOfWeek | count of crime",outputPath)
    var theft_crime_category = spark.sql("""select DayOfWeek,count(*) from crime_table where category = "LARCENY/THEFT" group by dayofweek order by count(*) desc""")
    comm.DFwriteToFileStringLong(theft_crime_category,outputPath)

    comm.writeToFileString("""There seems to be a variance inbetween days. Friday and saturday have higher values.
Not by a large margin but there appears to be a difference compared to the other days.
Let's see which group has the highest Coefficient Of Variation per day.""",outputPath)

    var rowsCV:List[(String,Double)] = List()
    var dfSubsetGrouped = spark.emptyDataFrame
    comm.writeToFileString("Find the crime group per-day-Coefficient Of Variation",outputPath)
    var distinctCategory  = crimeDF.select("Category").distinct().collect()
    distinctCategory.foreach(x=>{
      var dfSubset = crimeDF.filter(col("Category") === x.getString(0));
      var dfSubsetGrouped = dfSubset.groupBy("DayOfWeek","Category").count();
      //println(dfSubsetGrouped)
      var std = dfSubsetGrouped.agg(stddev_pop("count")).first.mkString.toDouble
      var mean = dfSubsetGrouped.agg(avg("count")).first.mkString.toDouble
      var cv = std / mean
      rowsCV :+=((x.getString(0),cv))
      comm.writeToFileString(x.getString(0)+" : "+cv,outputPath)
    })
    rowsCV.toDF("Category","cv").registerTempTable("variant_table")

    comm.writeToFileString("Top 5 Coefficient Of Variation by day:",outputPath)
    var Top5Variant = rowsCV.toDF("Category","cv").orderBy(desc("cv")).take(5)
    Top5Variant.foreach(x=>{
      var category  = x.getString(0);
      comm.writeToFileString(s"Showing $category : \nCategory | DayOfWeek | Count of Crime",outputPath)
      var display = spark.sql(
        s"""select category,dayofweek,count(*) from crime_table where category = "$category"
           |group by category,dayofweek order by category,count(*) desc  """.stripMargin)
      comm.DFwriteToFileStringStringLong(display,outputPath)
    })

    //comm.writeToFileString("Bottom 5 Coefficient Of Variation by day:",outputPath)
    //rowsCV.toDF("Category","cv").orderBy("cv").take(5)

    comm.writeToFileString("Plot crime categories per year .only display categories with a high coefficient of variation",outputPath)
    Top5Variant.foreach(x=>{
      var category  = x.getString(0);
      comm.writeToFileString(s"Showing $category : \n year_of_crime | count of crime",outputPath)
      var display = spark.sql(
        s"""select year(dates_new) as year_of_crime,count(*) from crime_table where category = "$category"
           |group by year(dates_new) order by count(*) desc  """.stripMargin)
      comm.DFwriteToFileIntLong(display,outputPath)
    })

    comm.writeToFileString("Plot crime categories per month .only display categories with a high coefficient of variation",outputPath)
    Top5Variant.foreach(x=>{
      var category  = x.getString(0);
      comm.writeToFileString(s"Showing $category : \n month_of_crime | count of crime",outputPath)
      var display = spark.sql(
        s"""select month(dates_new) as month_of_crime,count(*) from crime_table where category = "$category"
           |group by month(dates_new) order by count(*) desc  """.stripMargin)
      comm.DFwriteToFileIntLong(display,outputPath)
    })

    comm.writeToFileString("Getting List of Resolution Taken \n Resolution | Count",outputPath)
    var resolutions = spark.sql("select Resolution,count(*) from crime_table group by Resolution order by count(*) desc")
    comm.DFwriteToFileStringLong(resolutions,outputPath)

    comm.writeToFileString("Taking only resolution taken for top 5 variant category",outputPath)
    Top5Variant.foreach(x=>{
      var category  = x.getString(0);
      comm.writeToFileString(s"Showing $category : \n Resolution | count",outputPath)
      var display = spark.sql(
        s"""select resolution,count(resolution) from crime_table where category = "$category"
           |group by resolution order by count(resolution) desc  """.stripMargin)
      comm.DFwriteToFileStringLong(display,outputPath)
    })

    comm.writeToFileString("Plot crimes per district.only display categories with a high coefficient of variation",outputPath)
    Top5Variant.foreach(x=>{
      var category  = x.getString(0);
      comm.writeToFileString(s"Showing $category : \n District | Count of crime",outputPath)
      var display = spark.sql(
        s"""select pdDistrict,count(*) from crime_table where category = "$category"
           |group by pdDistrict order by count(*) desc  """.stripMargin)
      comm.DFwriteToFileStringLong(display,outputPath)
    })


  }
}
