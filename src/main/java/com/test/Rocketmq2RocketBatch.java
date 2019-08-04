package com.test;

import com.google.gson.JsonObject;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class Rocketmq2RocketBatch {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.shuffle.partitions", "4")
                .master("local[4]")
                .appName("RocketMQSourceProviderTest2")
                .getOrCreate();

        Dataset<Row> dfInput = spark
                .read()
                .format("org.apache.spark.sql.rocketmq.RocketMQSourceProvider")
                .option("nameServer", "localhost:9876")
                .option("topic", "spark-test")
                .option("pullBatchSize", "32")
                .option("pullTimeout", "5000")
                .load();

        Dataset<Row> dfOutput = dfInput.select("body");

        dfInput.printSchema();

        Dataset<Row> result = dfOutput
                .map(new MapFunction<Row, String>() {
                    @Override
                    public String call(Row value) throws Exception {
                        byte[] as = value.getAs(0);
                        return new String(as);
                    }
                }, Encoders.STRING())
                .toDF("body");

        result.show();

        result.write()
                .format("org.apache.spark.sql.rocketmq.RocketMQSourceProvider")
                .option("nameServer", "localhost:9876")
                .option("topic", "spark-sink-test")
                .save();


        StructType schema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("age", DataTypes.StringType, true, Metadata.empty()),
                new StructField("sex", DataTypes.StringType, true, Metadata.empty())
        });

        Dataset<Row> body = spark.read()
                .schema(schema)
                .format("csv")
                .option("path", "D:/workspacejava/structuredstreamingdemo/data")
                .load()
                .map(new MapFunction<Row, String>() {
                    @Override
                    public String call(Row value) throws Exception {
                        return Rocketmq2RocketBatch.transform(value);
                    }
                }, Encoders.STRING())
                .toDF("body");

        body.show();

        body.write()
                .format("org.apache.spark.sql.rocketmq.RocketMQSourceProvider")
                .option("nameServer", "localhost:9876")
                .option("topic", "spark-rmq")
                .save();
    }

    private static String transform(Row row) {
        JsonObject obj = new JsonObject();
        obj.addProperty("name", row.getString(0));
        obj.addProperty("age", row.getString(1));
        obj.addProperty("sex", row.getString(2));
        return obj.toString();
    }
}
