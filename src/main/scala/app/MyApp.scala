package app


import com.sparktest.readwrite.SparkReadWriteFiles
import org.apache.spark.sql.SparkSession

object MyApp extends App {

  System.setProperty("java.io.tmpdir","C:/temp")

  val nbRows = 100
  val spark2 = SparkSession.builder()
    .master("local[1]")
    .appName("test.com")
    .config("java.io.tmpdir","C:/temp")
    .getOrCreate();

  println("Spark Version : "+spark2.version)


  /*
  val df = spark.range(nbRows)
    .withColumn("x1", lit(1))
    .withColumn("x2", lit(2))
    .withColumn("c1", expr("id*3"))  
    .withColumn("c2", expr("id*3.14")) 
    .withColumn("c3", expr("id*1.2")) 
    .withColumn("c4", expr("id*3.33"))
  
  df.toTable("toto")
    
  spark.select("toto").show
*/

  /*
  val df2 = spark2.read.csv("src/main/resources/output_csv_full.csv")
  df2.printSchema()
  println(df2.count());
  df2.write.mode(SaveMode.Overwrite).parquet("src/main/resources/output/output_csv_full.parquet")

*/


  val temp = new SparkReadWriteFiles(spark2);
  temp.test(spark2)

}
