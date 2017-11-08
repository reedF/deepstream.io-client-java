package io.deepstream.testapp;

import java.util.concurrent.CyclicBarrier;

import io.deepstream.InvalidDeepstreamConfig;

public class TestPubSub {

    private static int LATCH_SIZE = 1;
    private static String prx = "/ws";
    private static String host = "vip3-ali-hangzhou-kefu-ecs-sdb-fanym-test1"; // 101.37.255.176
    private static int port = 6020; // localhost
    private static String url = host + prx;
    private static CyclicBarrier sub;

    public static void main(String[] args) throws InvalidDeepstreamConfig {

        if (args != null && args.length > 0) {
            LATCH_SIZE = Integer.parseInt(args[0]);
            if (args.length > 1) {
                host = args[1];
                port = Integer.parseInt(args[2]);
                url = host + ":" + port + prx;
            }

        }

        System.out.println(LATCH_SIZE);
        try {
            sub = new CyclicBarrier(LATCH_SIZE);
            for (int i = 0; i < LATCH_SIZE; i++) {
                String topic = "event/" + i;
                // new Subscriber.SubscriberApplication(topic);
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            sub.await();
                            new Publisher.PublisherApplication(topic, url);
                            new Subscriber.SubscriberApplication(topic, url);

                        } catch (Exception e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                }).start();

            }

            System.out.println("main await finished.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
