package io.deepstream.testapp;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import io.deepstream.AnonymousRecord;
import io.deepstream.ConfigOptions;
import io.deepstream.ConnectionState;
import io.deepstream.ConnectionStateListener;
import io.deepstream.DeepstreamClient;
import io.deepstream.DeepstreamError;
import io.deepstream.DeepstreamRuntimeErrorHandler;
import io.deepstream.Event;
import io.deepstream.EventListener;
import io.deepstream.HasResult;
import io.deepstream.InvalidDeepstreamConfig;
import io.deepstream.List;
import io.deepstream.ListChangedListener;
import io.deepstream.ListEntryChangedListener;
import io.deepstream.LoginResult;
import io.deepstream.PresenceEventListener;
import io.deepstream.Record;
import io.deepstream.RecordChangedCallback;
import io.deepstream.RpcResult;
import io.deepstream.SnapshotResult;
import io.deepstream.Topic;

public class Subscriber {
    // private static final String host = "192.168.59.103";
    public static String host = "localhost"; // 101.37.255.176
                                             // vip3-ali-hangzhou-kefu-ecs-sdb-fanym-test1
    public static int port = 6020; //

    public static void main(String[] args) throws InvalidDeepstreamConfig {
        // try {
        // doneSignal = new CountDownLatch(LATCH_SIZE);
        // for (int i = 0; i < LATCH_SIZE; i++) {
        // new SubscriberApplication();
        // }
        // System.out.println("main await begin.");
        // // "主线程"等待线程池中任务
        // doneSignal.await();
        // System.out.println("main await finished.");
        // } catch (Exception e) {
        // e.printStackTrace();
        // }
        new SubscriberApplication("event/a", host + ":" + port);
    }

    static class SubscriberApplication {
        private String topic;
        private String url;

        SubscriberApplication(String str1, String str2) throws InvalidDeepstreamConfig {
            topic = str1;
            url = str2;
            try {
                Map config = new HashMap<String, Object>();
                config.put(ConfigOptions.RECONNECT_INTERVAL_INCREMENT.toString(), 5000);
                config.put(ConfigOptions.MAX_RECONNECT_ATTEMPTS.toString(), 5);
                config.put(ConfigOptions.MAX_RECONNECT_INTERVAL.toString(), 1500);
                config.put(ConfigOptions.SUBSCRIPTION_TIMEOUT.toString(), 500);
                config.put(ConfigOptions.RECORD_READ_ACK_TIMEOUT.toString(), 500);
                config.put(ConfigOptions.RECORD_READ_TIMEOUT.toString(), 500);

                DeepstreamClient client = new DeepstreamClient(url, config);
                subscribeConnectionChanges(client);
                subscribeRuntimeErrors(client);

                JsonObject auth = new JsonObject();
                auth.add("username", new JsonPrimitive("test"));
                auth.add("password", new JsonPrimitive("1234"));
                LoginResult loginResult = client.login(auth);
                //LoginResult loginResult = client.login();
                if (!loginResult.loggedIn()) {
                    System.err.println("Failed to login " + loginResult.getErrorEvent());
                } else {
                    System.out.println("Login Success");
                    subscribeEvent(client);
                    // makeSnapshot(client, "record/snapshot");
                    // hasRecord(client);
                    // subscribeRecord(client, "record/b");
                    // subscribeAnonymousRecord(client);
                    // subscribeList(client);
                    // makeRpc(client);
                    // subscribePresence(client);
                    queryClients(client);

                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        private void hasRecord(final DeepstreamClient client) {
            HasResult hasResult = client.record.has("record/has");
            if (hasResult.hasError()) {
                System.err.println(String.format("Has did not work because: %s",
                        hasResult.getError().getMessage()));
            } else {
                System.out.println(String.format("Has result: %s", hasResult.getResult()));
            }
        }

        private void makeSnapshot(final DeepstreamClient client, final String recordName) {
            SnapshotResult snapshotResult = client.record.snapshot(recordName);
            if (snapshotResult.hasError()) {
                System.err.println(String.format("Snapshot did not work because: %s",
                        snapshotResult.getError().getMessage()));
            } else {
                System.out.println(String.format("Snapshot result: %s", snapshotResult.getData()));
            }
        }

        private void makeRpc(final DeepstreamClient client) {
            ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    RpcResult rpcResponse = client.rpc.make("add-numbers", new Double[] {
                            Math.floor(Math.random() * 10), Math.floor(Math.random() * 10)});
                    if (rpcResponse.success() == false) {
                        System.out.println(
                                String.format("RPC failed with data: %s", rpcResponse.getData()));
                    } else {
                        System.out.println(
                                String.format("RPC success with data: %s", rpcResponse.getData()));
                    }
                }
            }, 1, 5, TimeUnit.SECONDS);
        }

        private void subscribeRuntimeErrors(DeepstreamClient client) {
            client.setRuntimeErrorHandler(new DeepstreamRuntimeErrorHandler() {
                @Override
                public void onException(Topic topic, Event event, String errorMessage) {
                    System.out.println(
                            String.format("SUB Error occured %s %s %s", topic, event,
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

        private void subscribeAnonymousRecord(DeepstreamClient client) {
            AnonymousRecord anonymousRecord = client.record.getAnonymousRecord();
        }

        private void subscribeList(final DeepstreamClient client) {
            List list = client.record.getList("list/a");
            list.subscribe(new ListChangedListener() {
                @Override
                public void onListChanged(String listName, String[] entries) {
                    System.out.println(String.format("List %s entries changed to %s", listName,
                            Arrays.toString(entries)));
                }
            });
            list.subscribe(new ListEntryChangedListener() {
                @Override
                public void onEntryAdded(String listName, String entry, int position) {
                    System.out.println(String.format("List %s entry %s added", listName, entry));
                }

                @Override
                public void onEntryRemoved(String listName, String entry, int position) {
                    System.out.println(String.format("List %s entry %s removed", listName, entry));
                }

                @Override
                public void onEntryMoved(String listName, String entry, int position) {
                    System.out.println(String.format("List %s entry %s moved", listName, entry));
                }
            });
            System.out.println(String.format("List '%s' initial state: %s", list.name(),
                    Arrays.toString(list.getEntries())));
        }

        private void subscribeRecord(final DeepstreamClient client, final String recordName) {
            Record record = client.record.getRecord(recordName);
            record.subscribe(new RecordChangedCallback() {
                @Override
                public void onRecordChanged(String recordName, JsonElement data) {
                    System.out.println(String.format("Record '%s' changed, data is now: %s",
                            recordName, data));
                }
            });
            System.out.println(
                    String.format("Record '%s' initial state: ", record.name(), record.get()));
        }

        private void subscribeEvent(DeepstreamClient client) {
            client.event.subscribe(topic, new EventListener() {
                @Override
                public void onEvent(String eventName, Object args) {
                    String parameters = null;

                    if (args instanceof String) {
                        parameters = (String) args;
                    } else {
                        String str = Arrays.toString((Object[]) args);
                        parameters = str;
                    }
                    System.out.println(
                            String.format("Event '%s' occurred msg: %s", eventName, parameters));

                }
            });
            // client.event.emit(topic, new Object[] {"An event just happened", new
            // Date().getTime()});
        }

        private void subscribePresence(DeepstreamClient client) {
            client.presence.subscribe(new PresenceEventListener() {
                @Override
                public void onClientLogin(String username) {
                    System.out.println(username + " logged in");
                }

                @Override
                public void onClientLogout(String username) {
                    System.out.println(username + " logged out");
                }
            });
        }

        private void queryClients(DeepstreamClient client) throws DeepstreamError {
            String[] clients = client.presence.getAll();
            System.out.println(
                    String.format("Clients currently connected: %s", Arrays.toString(clients)));
        }

        private void publishEvent(DeepstreamClient client) {
            ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
            executorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    client.event.emit(topic,
                            new Object[] {"An event just happened", new Date().getTime()});
                }
            }, 5, 10, TimeUnit.SECONDS);
        }

    }
}
