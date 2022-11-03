package io.keepcoding.spark.exercise.streaming

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

import scala.concurrent.duration.Duration

object AntennaStreamingJob extends StreamingJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Final Project SQL Streaming")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()

  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val jsonSchema = StructType(Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false),
      StructField("bytes", LongType, nullable = false),
      StructField("app", StringType, nullable = false),
      )
    )

    dataFrame
      .select(from_json($"value".cast(StringType), jsonSchema).as("json"))
      .select($"json.*")

  }

  override def readUserdata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {

    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichUserWithMetadata(userDF: DataFrame, userdataDF: DataFrame): DataFrame = {
    userDF.as("a")
      .join(
        userdataDF.as("b"),
        $"a.id" === $"b.id"
      )
      .drop($"b.id")

  }

  override def computeBytesCount(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"antenna_id", $"bytes")
      .withWatermark("timestamp", "10 seconds")
      .groupBy($"antenna_id", window($"timestamp", "30 seconds"))
      .sum("bytes")


  }


  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future{

    dataFrame
      .writeStream
      .foreachBatch {
        (batch: DataFrame, _: Long) => {
          batch
            .write
            .mode(SaveMode.Append)
            .format("jdbc")
            .option("url", jdbcURI)
            .option("dbtable", jdbcTable)
            .option("user", user)
            .option("password", password)
            .save()
        }
      }
      .start()
      .awaitTermination()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
  dataFrame
    .select(
      $"timestamp", $"id", $"antenna_id", $"bytes", $"app",
      year($"timestamp").as("año"),
      month($"timestamp").as("mes"),
      dayofmonth($"timestamp").as("dia"),
      hour($"timestamp").as("hora")
    )

    .writeStream
    .format("parquet")
    .option("path", s"$storageRootPath/data")
    .option("checkpointLocation", s"$storageRootPath/checkpoint")
    .partitionBy("año", "mes", "dia", "hora")
    .start
    .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    //run(args)
    val kafkaDF = readFromKafka("34.125.209.162:9092", "devices")
    val parsedDF = parserJsonData(kafkaDF)

    val storageFuture = writeToStorage(parsedDF, "/tmp/antenna_parquet")

    val userDataDF = readUserdata(
      "jdbc:postgresql://34.122.136.252:5432/postgres",
      "user_metadata",
      "postgres",
      "keepcoding"
    )

    val enrichDF = enrichUserWithMetadata(parsedDF, userDataDF)

    val countByAntenna = computeBytesCount(enrichDF)

    val jdbcFuture = writeToJdbc(enrichDF, "jdbc:postgresql://34.122.136.252:5432/postgres", "antenna_agg", "postgres", "keepcoding")
    //antenna_agg es un tabla que contiene los valores agregados calculados: bytes recibidos por antena en una ventana de 5 minutos

    Await.result(
      Future.sequence(Seq(storageFuture, jdbcFuture)), Duration.Inf
      )

    spark.close()

    /*countByAntenna
      .writeStream
      .format("console")
      .start()
      .awaitTermination()*/


  }
}
