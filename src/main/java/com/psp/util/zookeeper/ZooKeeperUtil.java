package com.psp.util.zookeeper;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import com.psp.excception.zookeeper.ZookeeperInitializationException;

public class ZooKeeperUtil {
  
  private static ZooKeeper      zk;
  private static CountDownLatch connSignal                 = new CountDownLatch(0);
  
  public static final String    ZK_SERVERS_PROPERTY        = InitializeZookeeper.getZkServers();
  public static final String    ZK_FOREGROUND_LOCK_PREFIX  = InitializeZookeeper.getZkForegoundLockPrefix();
  public static final String    ZK_BACKGROUND_LOCK_PREFIX  = InitializeZookeeper.getZkBackgoundLockPrefix();
  public static final Integer   ZK_SESSION_TIMEOUT         = InitializeZookeeper.getZkSessionTimeout();
  public static final Integer   ZK_CONNECTION_RETRY_COUNT  = InitializeZookeeper.getZkConnectionRetryCount();
  public static final Long      ZK_FOREGROUND_TASK_TIMEOUT = InitializeZookeeper.getZkForegroundTaskTimeout();
  public static final Long      ZK_BACKGROUND_TASK_TIMEOUT = InitializeZookeeper.getZkBackgroundTaskTimeout();
  public static final Long      ZK_CONNECTION_RETRY_DELAY  = InitializeZookeeper.getZkConnectionRetryDelay();
  
  static ZooKeeper connect() throws Exception
  {
    
    zk = new ZooKeeper(ZK_SERVERS_PROPERTY, ZK_SESSION_TIMEOUT, new Watcher()
    {
      
      public void process(WatchedEvent event)
      {
        if (event.getState() == KeeperState.SyncConnected) {
          connSignal.countDown();
        }
      }
    });
    
    connSignal.await();
    
    return zk;
  }
  
  static void setZookeeper(ZooKeeper zk)
  {
    ZooKeeperUtil.zk = zk;
  }
  
  public static ZooKeeper getZookeeper() throws Exception
  {
    if (zk != null) {
      return zk;
    }
    else {
      throw new ZookeeperInitializationException("Zookeeper connection is not initialized");
    }
  }
  
  public static String getLockPathByLockName(String lockName)
  {
    return "/" + lockName;
  }
  
  public static byte[] getDataBytes(String data)
  {
    return data.getBytes();
  }
  
  public static String convertBytesToString(byte[] dataBytes)
  {
    String data = null;
    try {
      data = new String(dataBytes, "UTF-8");
    }
    catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return data;
  }

  public static void deleteLock(List<String> locks)
  {
    List<String> lockList = new ArrayList<>(locks);
    try {
      ZooKeeper zookeeper = getZookeeper();
      for(String lock : lockList) {
        zookeeper.delete(lock, -1);
      }
    }
    catch(NoNodeException e) {
      
    }
    catch (Exception e) {
      System.out.println(Thread.currentThread().getName() + " **********  Unable to delete locks : " + locks);
      e.printStackTrace();
    }
  }
}
