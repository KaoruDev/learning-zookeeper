package com.kaoruk;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by kaoru on 2/3/17.
 */
public class Executor implements Watcher {
    ZooKeeper zk;
    String zNode;
    ReentrantLock lock;
    volatile boolean alive = true;
    static ExecutorService pool = Executors.newFixedThreadPool(3);

    public static void main(String[] args) throws IOException {
       Runnable run = () -> {
           try {
               new Executor();
           } catch (IOException e) {
               System.out.println("Executor threw an IO Exeception");
               e.printStackTrace();
           }
       };

       pool.execute(run);
    }

    public Executor() throws IOException {
        this.zk = new ZooKeeper("localhost", 3000, this);
        this.zNode = "/learning_zoo";
        this.lock = new ReentrantLock();
    }

    public void process(WatchedEvent event) {
        System.out.println(Thread.currentThread().getName());
        System.out.println("event type: " + event.getType());
        System.out.println("event state: " + event.getState());
        System.out.println("event path: " + event.getPath());
        if (event.getState() == Event.KeeperState.Expired) {
            stop();
        } else if (event.getState() == Event.KeeperState.SyncConnected) {
            zk.exists(zNode, true, new EventHandler(this), null);
        } else {
            System.out.println("event state: " + event.getState());
        }
    }

    public void stop() {
        this.alive = false;
        pool.shutdown();
    }

    byte[] getZNodeData() throws KeeperException, InterruptedException {
        return zk.getData(zNode, false, null);
    }

    public static class EventHandler implements AsyncCallback.StatCallback {
        Executor executor;

        public EventHandler(Executor executor) {
            this.executor = executor;
        }

        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            boolean exists = false;
            System.out.println("Processing Results: rc: " + rc);

            switch(Code.get(rc)) {
                case OK:
                    exists = true;
                    break;
                case NONODE:
                case SESSIONEXPIRED:
                case NOAUTH:
                    executor.stop();
                    return;
                default:
                    // retry
                    return;
            }

            System.out.println("exists: " + exists);

            byte b[] = null;
            if (exists) {
                try {
                    b = executor.getZNodeData();
                } catch(KeeperException e) {
                    e.printStackTrace();
                } catch(InterruptedException e) {
                    return;
                }
            }

            if (!Objects.isNull(b)) {
                System.out.println(new String(b, StandardCharsets.UTF_8));
            }
        }
    }
}
