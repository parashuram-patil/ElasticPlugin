package com.psp.zookeeper.configuration;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZkDistributedLock {
  
  private final ZooKeeper zk;
  private String          parentLockPath;
  private String          lockPath;
  private String          lockMessage;
  
  public ZkDistributedLock(String lockMessage, ZooKeeper zk, String parentNode)
  {
    this.zk = zk;
    this.parentLockPath = "/" + parentNode;
    this.lockMessage = lockMessage;
  }
  
  public void lock() throws Exception
  {
    try {
      
      parentLockPath = getOrCreateParentNode();
      
      long sessionId = zk.getSessionId();
      String lockName = "x-" + sessionId + "-";
      lockPath = zk.create(parentLockPath + "/" + lockName, lockMessage.getBytes(),
          Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
      final Object lock = new Object();
      synchronized (lock) {
        while (true) {
          List<String> nodes = zk.getChildren(parentLockPath, new Watcher()
          {
            
            @Override
            public void process(WatchedEvent event)
            {
              synchronized (lock) {
                lock.notifyAll();
              }
            }
          });
          
          SortedSet<String> set = new TreeSet<>();
          for (String node : nodes) {
            set.add(node);
          }
          
          if (lockPath.endsWith(set.first())) {
            return;
          }
          else {
            lock.wait();
          }
        }
      }
    }
    catch (KeeperException e) {
      throw new IOException(e);
    }
    catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
  
  private String getOrCreateParentNode() throws Exception
  {
    Stat stat = zk.exists(parentLockPath, false);
    if (stat != null) {
      return parentLockPath;
    }
    parentLockPath = zk.create(parentLockPath, "Parent-Node-Created-On-Child-Request".getBytes(),
        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    return parentLockPath;
  }
  
  public void unlock() throws IOException
  {
    try {
      zk.delete(lockPath, -1);
      lockPath = null;
    }
    catch (KeeperException e) {
      throw new IOException(e);
    }
    catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}