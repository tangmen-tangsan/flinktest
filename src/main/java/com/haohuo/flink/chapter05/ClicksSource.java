package com.haohuo.flink.chapter05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClicksSource implements SourceFunction<Event> {
    private boolean flag = true;
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();

        String[] users= {"tom","Jack","oli","anlis","Eson"};
        String[] urls = {"./home","./detile","./cart","prod/?id=100","prod/?id=10"};

        while (flag){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user,url,timestamp));
            Thread.sleep(1000L);

        }
    }

    @Override
    public void cancel() {
         flag= false;

    }
}
