package io.deepstream.testapp;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import io.deepstream.*;

import java.util.Date;
import java.util.concurrent.*;

public class Publisher {

    // private static final String host = "192.168.59.103";
    public static String host = "localhost"; // 101.37.255.176 vip3-ali-hangzhou-kefu-ecs-sdb-fanym-test1
    public static int port = 6020; // 

    public static void main(String[] args) throws InvalidDeepstreamConfig, InterruptedException {
        new PublisherApplication("event/.*", host + ":" + port);
    }

    static class PublisherApplication {
        private String topic;
        private String url;

        PublisherApplication(String str1, String str2) throws InvalidDeepstreamConfig {
            topic = str1;
            url = str2;
            try {
                DeepstreamClient client =
                        DeepstreamFactory.getInstance().getClient(url);
                subscribeConnectionChanges(client);
                subscribeRuntimeErrors(client);

                JsonObject auth = new JsonObject();
                auth.add("username", new JsonPrimitive("pub"));
                auth.add("password", new JsonPrimitive("1234"));
                LoginResult loginResult = client.login(auth);
                //LoginResult loginResult = client.login();
                if (!loginResult.loggedIn()) {
                    System.err.println("Provider Failed to login " + loginResult.getErrorEvent());
                } else {
                    System.out.println("Provider Login Success");
                    listenEvent(client);
                    // listenRecord(client);
                    // listenList(client);
                    // provideRpc(client);
                    // updateRecordWithAck("testRecord", client);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        private void listenList(final DeepstreamClient client) {
            final ScheduledFuture[] scheduledFuture = new ScheduledFuture[1];
            client.record.listen("list/.*", new ListenListener() {
                @Override
                public boolean onSubscriptionForPatternAdded(final String subscription) {
                    System.out.println(String.format("List %s just subscribed.", subscription));

                    ExecutorService executor = Executors.newSingleThreadExecutor();
                    executor.execute(new Runnable() {
                        @Override
                        public void run() {
                            updateList(subscription, client);
                        }
                    });

                    return true;
                }

                @Override
                public void onSubscriptionForPatternRemoved(String subscription) {
                    System.out.println(String.format("List %s just unsubscribed.", subscription));
                    scheduledFuture[0].cancel(false);
                }
            });
        }

        private void listenRecord(final DeepstreamClient client) {
            final ScheduledFuture[] scheduledFuture = new ScheduledFuture[1];
            client.record.listen("record/.*", new ListenListener() {
                @Override
                public boolean onSubscriptionForPatternAdded(final String subscription) {
                    System.out.println(String.format("Record %s just subscribed.", subscription));

                    ExecutorService executor = Executors.newSingleThreadExecutor();
                    executor.execute(new Runnable() {
                        @Override
                        public void run() {
                            updateRecord(subscription, client);
                        }
                    });

                    return true;
                }

                @Override
                public void onSubscriptionForPatternRemoved(String subscription) {
                    System.out.println(String.format("Record %s just unsubscribed.", subscription));
                    scheduledFuture[0].cancel(false);
                }
            });
        }

        private ScheduledFuture updateRecord(final String subscription, DeepstreamClient client) {
            final Record record = client.record.getRecord(subscription);
            final int[] count = {0};
            ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);
            ScheduledFuture scheduledFuture = executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    JsonObject data = new JsonObject();
                    data.addProperty("time", new Date().getTime());
                    data.addProperty("id", subscription);
                    data.addProperty("count", count[0]++);
                    record.set(data);
                    System.out.println("Setting record " + data);
                }
            }, 1, 5, TimeUnit.SECONDS);
            return scheduledFuture;
        }

        private ScheduledFuture updateList(final String subscription,
                final DeepstreamClient client) {
            final List list = client.record.getList(subscription);
            list.setEntries(new String[] {});
            ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);
            ScheduledFuture scheduledFuture = executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    list.addEntry(client.getUid());
                }
            }, 1, 5, TimeUnit.SECONDS);
            return scheduledFuture;
        }

        private void updateRecordWithAck(String recordName, DeepstreamClient client) {
            Record record = client.record.getRecord(recordName);
            RecordSetResult result = record.setWithAck("number", 23);
            String error = result.getResult();
            if (error == null) {
                System.out.println("Record set successfully with ack");
            } else {
                System.out.println("Record wasn't able to be set, error: " + error);
            }
        }

        private void listenEvent(final DeepstreamClient client) {
            final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
            // publishEvent("event/a", client, executorService);
            client.event.listen("event/.*", new ListenListener() {
                @Override
                public boolean onSubscriptionForPatternAdded(final String subscription) {
                    System.out
                            .println(String.format("Event %s just subscribed.", subscription));
                    publishEvent(subscription, client, executorService);
                    return true;
                }

                @Override
                public void onSubscriptionForPatternRemoved(String subscription) {
                    System.out.println(String.format("Event %s just unsubscribed.", subscription));
                }
            });
        }

        private void publishEvent(final String subscription, final DeepstreamClient client,
                ScheduledExecutorService executorService) {
            executorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    client.event.emit(subscription,
                            new String("An event just happened" + new Date().getTime()));
                }
            }, 1, 5, TimeUnit.SECONDS);
        }

        private void provideRpc(final DeepstreamClient client) {
            client.rpc.provide("add-numbers", new RpcRequestedListener() {
                public void onRPCRequested(String rpcName, Object data, RpcResponse response) {
                    System.out.println("Got an RPC request");
                    JsonArray numbers = (JsonArray) data;
                    double random = Math.random();
                    if (random < 0.2) {
                        response.reject();
                    } else if (random < 0.7) {
                        response.send(numbers.get(0).getAsDouble() + numbers.get(1).getAsDouble());
                    } else {
                        response.error("This intentionally randomly failed");
                    }
                }
            });
        }

        private void subscribeRuntimeErrors(DeepstreamClient client) {
            client.setRuntimeErrorHandler(new DeepstreamRuntimeErrorHandler() {
                @Override
                public void onException(Topic topic, Event event, String errorMessage) {
                    System.out.println(
                            String.format("PUB Error occured %s %s %s", topic, event,
                                    errorMessage));
                }
            });
        }

        private void subscribeConnectionChanges(DeepstreamClient client) {
            client.addConnectionChangeListener(new ConnectionStateListener() {
                @Override
                public void connectionStateChanged(ConnectionState connectionState) {
                    System.out.println("Connection state changed " + connectionState);
                }
            });
        }

    }
}
