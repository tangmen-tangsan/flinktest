package com.haohuo.flink.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformationFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Bob", "./home", 1000l),
                new Event("jack", "./detail", 2000l),
                new Event("mary", "./page", 3000l));
//使用自定义类，实现FilterFuntion
        stream.filter(new MyFilter()).print();


        //使用匿名函数
        SingleOutputStreamOperator<Event> stream1 = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("jack");
            }
        });
        stream1.print("1");
        //使用lamda表达式
        stream.filter(data->data.user.equals("jack")).print("2");


        env.execute();
    }
    public static class MyFilter implements FilterFunction<Event>{

        @Override
        public boolean filter(Event value) throws Exception {
            return value.user.equals("Bob");
        }
    }
}
