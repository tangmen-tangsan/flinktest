package com.haohuo.flink.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TranformMapTest {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素中获取数据
        DataStreamSource<Event> stream = env.fromElements(new Event("Bob", "./home", 1000l),
                new Event("jack", "./detail", 2000l),
                new Event("mary", "./page", 3000l));

        //使用自定义类，实现MapFunction
        SingleOutputStreamOperator<String> map = stream.map(new MyMap());
//        map.print();

        //使用匿名类
        SingleOutputStreamOperator<String> map1 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.user;
            }
        });
//        map1.print();

        //使用lamda表达式
        SingleOutputStreamOperator<String> map2 = stream.map(data -> data.user);
        map.print();
        env.execute();



    }
    public static class MyMap implements MapFunction<Event,String>{

        @Override
        public String map(Event value) throws Exception {
            return value.user;
        }
    }
}
