package com.pasquier.jean.flink

import org.apache.flink.table.api._

object App {

  def main(args: Array[String]): Unit = {

    val source_table = "raw_measurements"
    val sink_table = "agg_measurements"

    val kafka_servers = "localhost:9092"

    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val env = TableEnvironment.create(settings)

    val source_query =
      s"""CREATE TABLE IF NOT EXISTS $source_table (
         |    device         VARCHAR(16),
         |    attribute      VARCHAR(16),
         |    ts             BIGINT,
         |    `value`        DOUBLE,
         |    ts_ltz         AS TO_TIMESTAMP_LTZ(ts, 3),
         |    WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '3' SECONDS
         |  )
         |  WITH (
         |    'connector'                     = 'kafka',
         |    'topic'                         = '$source_table',
         |    'properties.bootstrap.servers'  = '$kafka_servers',
         |    'properties.group.id'           = '$source_table-flink-group',
         |    'scan.startup.mode'             = 'earliest-offset',
         |    'format'                        = 'json'
         |  )""".stripMargin

    env.createTemporaryTable(
      source_table,
      TableDescriptor
        .forConnector("kafka")
        .format("json")
        .schema(
          Schema.newBuilder
            .column("device", DataTypes.VARCHAR(16))
            .column("attribute", DataTypes.VARCHAR(16))
            .column("ts", DataTypes.BIGINT())
            .column("value", DataTypes.DOUBLE())
            .columnByExpression("ts_ltz", "TO_TIMESTAMP_LTZ(ts, 3)") // toTimestampLtz($"ts", 3)
            .watermark("ts_ltz", "ts_ltz - INTERVAL '3' SECONDS") //  $"ts_ltz" - 5.seconds
            .build()
        )
        .option("topic", source_table)
        .option("properties.bootstrap.servers", kafka_servers)
        .option("properties.group.id", s"$source_table-flink-group")
        .option("scan.startup.mode", "earliest-offset")
        .build()
    )

//    println(s"Registering source:\n$source_query")
//    env.executeSql(source_query)

    val src = env.from(source_table)

    val agg = src
      .window(Slide.over(10.minutes).every(1.minute).on($"ts_ltz").as("time_bucket"))
      .groupBy(
        $"time_bucket",
        $"device",
        $"attribute"
      )
      .select(
        $"device",
        $"attribute",
        $"time_bucket".start.as("time_bucket"),
        $"value".avg.as("avg_op"),
        $"value".min.as("min_op"),
        $"value".max.as("max_op")
      )

    val sink_query =
      s"""CREATE TABLE IF NOT EXISTS $sink_table (
         |    device         VARCHAR(16),
         |    attribute      VARCHAR(16),
         |    time_bucket    TIMESTAMP_LTZ(3),
         |    avg_op         DOUBLE,
         |    min_op         DOUBLE,
         |    max_op         DOUBLE
         |  )
         |  WITH (
         |    'connector'                     = 'kafka',
         |    'topic'                         = '$sink_table',
         |    'properties.bootstrap.servers'  = '$kafka_servers',
         |    'format'                        = 'json'
         |  )""".stripMargin

    println(s"Registering sink:\n$sink_query")
    env.executeSql(sink_query)

    agg.executeInsert(sink_table)
  }

}
