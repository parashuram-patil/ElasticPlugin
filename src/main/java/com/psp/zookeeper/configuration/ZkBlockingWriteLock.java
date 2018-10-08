package com.psp.zookeeper.configuration;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import com.psp.excception.zookeeper.InstanceLockException;
import com.psp.util.zookeeper.ZooKeeperUtil;
import com.psp.zookeeper.lib.LockListener;
import com.psp.zookeeper.lib.WriteLock;
import com.psp.zookeeper.lib.WriteLock.LockTypes;

public class ZkBlockingWriteLock {
  
  // private static final Logger LOG = LoggerFactory.getLogger(BlockingWriteLock.class);
  
  private final byte[]           lockInfo;
  private final String           lockName;
  private final LockTypes        lockType;
  private final WriteLock        writeLock;
  private final CountDownLatch   lockAcquiredSignal    = new CountDownLatch(1);
  private long                   timeout               = 2l;
  private long                   foregroundTaskTimeout = ZooKeeperUtil.ZK_FOREGROUND_TASK_TIMEOUT;
  private long                   backgroundTaskTimeout = ZooKeeperUtil.ZK_BACKGROUND_TASK_TIMEOUT;
  private TimeUnit               unit                  = TimeUnit.MILLISECONDS;
  
  //private static ZooKeeper       zk;
  //private static CountDownLatch  connSignal         = new CountDownLatch(0);
  
  private static final List<ACL> DEFAULT_ACL        = ZooDefs.Ids.OPEN_ACL_UNSAFE;
  
  public ZkBlockingWriteLock(String lockInfo, ZooKeeper zooKeeper, String lockName) {
    this(lockInfo, zooKeeper, lockName, LockTypes.FOREGROUND_LOCK);
  }
  
  public ZkBlockingWriteLock(String lockInfo, ZooKeeper zooKeeper, String lockName, LockTypes lockType)
  {
    this(lockInfo, zooKeeper, lockName, lockType, DEFAULT_ACL);
  }
  
  public ZkBlockingWriteLock(String lockInfo, ZooKeeper zooKeeper, String lockName, LockTypes lockType, List<ACL> acl)
  {
    this.lockInfo = lockInfo.getBytes();
    this.lockName = ZooKeeperUtil.getLockPathByLockName(lockName);
    this.lockType = lockType;
    setTimeout(lockType);
    writeLock = new WriteLock(zooKeeper, this.lockName, this.lockInfo, this.lockType, acl, new SyncLockListener());
  }
  
  private void setTimeout(LockTypes lockType)
  {
    if (lockType.equals(LockTypes.FOREGROUND_LOCK)) {
      timeout = foregroundTaskTimeout;
    }
    else if (lockType.equals(LockTypes.BACKGROUND_LOCK)) {
      timeout = backgroundTaskTimeout;
    }
  }
  
  public void lock() throws InterruptedException, KeeperException, InstanceLockException
  {
    writeLock.lock();
    lockAcquiredSignal.await();
  }

  public void lockWithTimeout() throws InterruptedException, KeeperException, InstanceLockException
  {
    lockWithTimeout(this.timeout, this.unit);
  }
  
  private boolean lockWithTimeout(long timeout, TimeUnit unit) throws InterruptedException, KeeperException, InstanceLockException
  {
    writeLock.lock();
    boolean await = lockAcquiredSignal.await(timeout, unit);
    if(!await) {
      writeLock.unlock();
      throw new InstanceLockException();
    }
    return await;
  }
  
  public void unlock()
  {
    writeLock.unlock();
  }
  
  public String getLockName() {
    return writeLock.getId();
  }
  
  class SyncLockListener implements LockListener {
    
    private String entiyId;
    private String lockId;
    
    @Override
    public void lockAcquired()
    {
      //String time = new SimpleDateFormat("dd-MM-yyyy:HH:mm:ss.sss").format(new Date());
      //System.out.println("\nLock acquired by " + Thread.currentThread().getName() + " on product " + getEntiyId() + " at " + time);
      lockAcquiredSignal.countDown();
    }
    
    @Override
    public void lockReleased()
    {
      //String time = new SimpleDateFormat("dd-MM-yyyy:HH:mm:ss.sss").format(new Date());
      //System.out.println("\nLock released by " + Thread.currentThread().getName() + " on product " + getEntiyId() + " at " + time);
    }
    
    public void setEntiyId(String entiyId)
    {
      this.entiyId = entiyId;
    }
    
    public String getEntiyId()
    {
      return this.entiyId;
    }
    
    public void setLockId(String lockId)
    {
      this.lockId = lockId;
    }
    
    public String getLockId()
    {
      return this.lockId;
    }
  }
  
  /*public static ZooKeeper connect(String host) throws Exception
  {
    if (zk != null) {
      return zk;
    }
    
    zk = new ZooKeeper(host, 3000, new Watcher()
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
  }*/
}
