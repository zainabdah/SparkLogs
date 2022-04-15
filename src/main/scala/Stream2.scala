import Streaming.{spark, streamDF}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object Stream2 extends App {


    val schemaT= StructType(List(

      StructField("protocole",StringType,true),
      StructField("status",StringType,true),
      StructField("url",StringType,true),
      StructField("path",StringType,true),
      StructField("id",StringType,true)
    ))

    val sparkSession = SparkSession.builder()
      .config(ConfigurationOptions.ES_NODES, "localhost")
      .config(ConfigurationOptions.ES_PORT, "9200")
      .master("local[*]")
      .appName("sample-structured-streaming")
      .getOrCreate()

    val streamDF = sparkSession.readStream.option("delimiter"," ").schema(schemaT).csv("/home/dba/IdeaProjects/SparkLogs/src/main/scala/logs/Logs")
        streamDF.createOrReplaceTempView("SDF")
    val outDF = sparkSession.sql("select * from SDF")

    outDF
      .writeStream
      .outputMode("append")
      .format("org.elasticsearch.spark.sql")
      .option("checkpointLocation", "/home/dba/IdeaProjects/SparkLogs/src/main/scala/temp")
      .start("sparklogs/logs").awaitTermination()







}
