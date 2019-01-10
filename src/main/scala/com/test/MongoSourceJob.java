package com.test;

import com.alibaba.fastjson.JSON;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;


public class MongoSourceJob {


    public static final String MONGO_URI = "mongodb://172.16.2.45:27017/tmp.bairong_credit";

    public static void main(String[] args) throws Exception {
        String condition = "{'BD@Mz@uuid':'589B20AA96714141B447D3E3F0E7A42E'}";
        //创建运行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //将mongo数据转化为Hadoop数据格式
        HadoopInputFormat<BSONWritable, BSONWritable> hdIf =
                new HadoopInputFormat<>(new MongoInputFormat(), BSONWritable.class, BSONWritable.class, new JobConf());
        hdIf.getJobConf().set("mongo.input.split.create_input_splits", "false");
        hdIf.getJobConf().set("mongo.input.uri", MONGO_URI);
        hdIf.getJobConf().set("mongo.input.query", condition);


        long count = env.createInput(hdIf).map((MapFunction<Tuple2<BSONWritable, BSONWritable>, String>) value -> {
            BSONWritable v = value.getField(1);
            return JSON.parseObject(v.getDoc().toString()).toJSONString();
        }).count();


        System.out.println("总共读取到{}条MongoDB数据 count:" + count);


        List<String> result = env.createInput(hdIf).map((MapFunction<Tuple2<BSONWritable, BSONWritable>, String>) value -> {
            BSONWritable v = value.getField(1);
            return JSON.parseObject(v.getDoc().toString()).toJSONString();
        }).collect();


        System.out.println(result);
    }
}