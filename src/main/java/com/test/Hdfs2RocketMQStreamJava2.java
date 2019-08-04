package com.test;

import com.google.gson.JsonObject;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Hdfs2RocketMQStreamJava2 {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.streaming.checkpointLocation", "checkpoint")
                .master("local[4]")
                .appName("Hdfs2RocketMQStreamJava")
                .getOrCreate();

        StructType schema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("age", DataTypes.StringType, true, Metadata.empty()),
                new StructField("sex", DataTypes.StringType, true, Metadata.empty())
        });

        Dataset<Row> body = spark.readStream()
                .schema(schema)
                .csv("D:/workspacejava/structuredstreamingdemo/data")
                .map(new MapFunction<Row, String>() {
                    @Override
                    public String call(Row value) throws Exception {
                        return transform(value);
                    }
                }, Encoders.STRING())
                .toDF("body");

        StreamingQuery query = body.writeStream()
                .outputMode(OutputMode.Append())
                .format("org.apache.spark.sql.rocketmq.RocketMQSourceProvider")
                .option("nameServer", "localhost:9876")
                .option("topic", "spark-rmq2")
                .trigger(Trigger.ProcessingTime("1 second"))
                .start();

        query.stop();

    }

    private static String transform(Row row) {
        JsonObject obj = new JsonObject();
        obj.addProperty("name", row.getString(0));
        obj.addProperty("age", row.getString(1));
        obj.addProperty("sex", row.getString(2));
        return obj.toString();
    }
}
