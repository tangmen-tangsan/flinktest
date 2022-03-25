package com.haohuo.flink.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import javax.lang.model.element.VariableElement;
import java.util.ArrayList;
import java.util.Properties;

public class SouseTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.从文件中读取数据
        DataStreamSource<String> textFileStream = env.readTextFile("Input/clicks.txt");
        textFileStream.print("txt");
        //2.从集合中读取数据
        ArrayList<Integer> num = new ArrayList<>();
        num.add(1);
        num.add(2);
        DataStreamSource<Integer> integerDataStreamSource = env.fromCollection(num);
        integerDataStreamSource.print("1");

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("tom","ditile",1000l));
        DataStreamSource<Event> eventDataStreamSource = env.fromCollection(events);
        eventDataStreamSource.print("2");

        //3.从kafka读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "lianjiwei102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> clicks = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));
        clicks.print();

        env.execute();
    }


}
