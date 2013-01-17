package com.ebay.chluo.hdfs.zookeeper.client;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZooKeeperClientTest {
   private ZooKeeper zk = null;

   private String parentPath = "/test";

   private String path = "/test/client";

   private List<ACL> acls = ZooDefs.Ids.OPEN_ACL_UNSAFE;

   @Before
   public void setUpBeforeClass() throws Exception {
      zk = new ZooKeeper("127.0.0.1:2181", 1000, new ZooKeeperWatcher());
   }

   @After
   public void tearDownAfterClass() throws Exception {
      zk.close();
   }

   @Test
   public void testClient() {

      try {
         if (zk.exists(parentPath, null) == null) {
            zk.create(parentPath, null, acls, CreateMode.PERSISTENT);
         }
         zk.create(path, "client".getBytes(), acls, CreateMode.EPHEMERAL);
         Stat stat = new Stat();
         String value = new String(zk.getData(path, new TestWatcher(), stat));
         assertEquals("client", value);
         zk.setData(path, "new client".getBytes(), stat.getVersion());
      } catch (KeeperException e) {
         e.printStackTrace();
      } catch (InterruptedException e) {
         e.printStackTrace();
      }
   }

   @Test
   public void testDisconnect() {
      try {
         ZooKeeper zk2 = new ZooKeeper("127.0.0.1:2181", 1000, new ZooKeeperWatcher());
         zk2.create("/connect", null, acls, CreateMode.EPHEMERAL);
         zk.exists("/connect", new ConnectWatcher());
         zk2.close();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   class TestWatcher implements Watcher {
      public void process(WatchedEvent event) {
         EventType type = event.getType();
         if (type == EventType.NodeDataChanged) {
            String path = event.getPath();
            System.out.println(path + " changed");
            try {
               String value = new String(zk.getData(path, null, null));
               assertEquals("new client", value);
            } catch (KeeperException e) {
               e.printStackTrace();
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
         }
      }
   }

   class ConnectWatcher implements Watcher {
      public void process(WatchedEvent event) {
         EventType type = event.getType();
         if (type == EventType.NodeDeleted) {
            String path = event.getPath();
            System.out.println(path + " deleted");
         }
      }
   }

   class ZooKeeperWatcher implements Watcher {
      public void process(WatchedEvent event) {
         System.out.println(event.getState());
      }
   }
}
