/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.psp.zookeeper.lib;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.psp.util.zookeeper.ZooKeeperUtil;

/**
 * Copied directly from the ZooKeeper lock recipe, and modified slightly (e.g. for Sonar rule violations).
 * <p>
 * A <a href="package.html">protocol to implement an exclusive
 * write lock or to elect a leader</a>. <p/> You invoke {@link #lock()} to
 * start the process of grabbing the lock; you may get the lock then or it may be
 * some time later. <p/> You can register a listener so that you are invoked
 * when you get the lock; otherwise you can ask if you have the lock
 * by calling {@link #isOwner()}
 */
public class WriteLock extends ProtocolSupport {
  
    //private static final Logger LOG = LoggerFactory.getLogger(WriteLock.class);

  private final String                 dir;
  private String                       id;
  private ZNodeName                    idName;
  private String                       ownerId;
  private String                       lastChildId;
  private final byte[]                 data;
  private LockListener                 callback;
  private final LockZooKeeperOperation zop;
  private LockTypes                    lockType;
  private final String                 PRIVATE_LOCK_NAME = "privateLock";
  private final String                 PRIVATE_LOCK_PATH;
  private boolean                      hasPrivateLock    = false;     
  
  public enum LockTypes
  {
    FOREGROUND_LOCK("FOREGROUND_LOCK", ZooKeeperUtil.ZK_FOREGROUND_LOCK_PREFIX),
    BACKGROUND_LOCK("BACKGROUND_LOCK", ZooKeeperUtil.ZK_BACKGROUND_LOCK_PREFIX);
    
    private final String key;
    private final String value;
    
    LockTypes(String key, String value)
    {
      this.key = key;
      this.value = value;
    }
    
    public String getKey()
    {
      return key;
    }
    
    public String getValue()
    {
      return value;
    }
  }
  
  public enum UnlockTypes
  {
    ALL("ALL", "ALL"), 
    PRIVATE_LOCK("PRIVATE_LOCK", "PRIVATE_LOCK"),
    SYNC_LOCK("SYNC_LOCK", "SYNC_LOCK");
    
    private final String key;
    private final String value;
    
    UnlockTypes(String key, String value)
    {
      this.key = key;
      this.value = value;
    }
    
    public String getKey()
    {
      return key;
    }
    
    public String getValue()
    {
      return value;
    }
  }

  /**
   * zookeeper contructor for writelock
   *
   * @param zookeeper zookeeper client instance
   * @param dir       the parent path you want to use for locking
   * @param acl       the acl that you want to use for all the paths,
   *                  if null world read/write is used.
   */
  public WriteLock(ZooKeeper zookeeper, String dir, byte[] data, LockTypes lockType, List<ACL> acl)
  {
    super(zookeeper);
    this.dir = dir;
    this.data = data;
    this.lockType = lockType;
    this.PRIVATE_LOCK_PATH = dir +  ZooKeeperUtil.getLockPathByLockName(PRIVATE_LOCK_NAME);
    if (acl != null) {
      setAcl(acl);
    }
    this.zop = new LockZooKeeperOperation();
  }

  /**
   * zookeeper contructor for writelock with callback
   *
   * @param zookeeper the zookeeper client instance
   * @param dir       the parent path you want to use for locking
   * @param acl       the acls that you want to use for all the paths
   * @param callback  the call back instance
   */
  public WriteLock(ZooKeeper zookeeper, String dir, byte[] data, LockTypes lockType, List<ACL> acl,
      LockListener callback)
  {
    this(zookeeper, dir, data, lockType, acl);
    this.callback = callback;
  }

    /**
     * return the current locklistener
     *
     * @return the locklistener
     */
    public LockListener getLockListener() {
        return this.callback;
    }

    /**
     * register a different call back listener
     *
     * @param callback the call back instance
     */
    public void setLockListener(LockListener callback) {
        this.callback = callback;
    }

    /**
     * Removes the lock or associated znode if
     * you no longer require the lock. this also
     * removes your request in the queue for locking
     * in case you do not already hold the lock.
     *
     * @throws RuntimeException throws a runtime exception
     *                          if it cannot connect to zookeeper.
     */
    public synchronized void unlock() throws RuntimeException
    {
      if (!isClosed() && id != null) {
            // we don't need to retry this operation in the case of failure
            // as ZK will remove ephemeral files and we don't wanna hang
            // this process when closing if we cannot reconnect to ZK
            try {
              
              ZooKeeperOperation zopdel = () -> {
                deleteZnode(zookeeper, id, -1);
                if(hasPrivateLock) {
                  deleteZnode(zookeeper, PRIVATE_LOCK_PATH, -1);
                }
                return Boolean.TRUE;
              };
              zopdel.execute();
            } catch (InterruptedException e) {
                //LOG.warn("Interrupted", e);
                //set that we have been interrupted.
                Thread.currentThread().interrupt();
            } catch (KeeperException.NoNodeException e) {
                // do nothing
                //LOG.trace("No node", e);
            } catch (KeeperException e) {
                //LOG.warn("KeeperException: {}", e.code(), e);
                throw new RuntimeException(e.getMessage(), e);
            } finally {
                if (callback != null) {
                  callback.setEntiyId(dir);
                  callback.setLockId(id);
                  callback.lockReleased();
                }
                //id = null;
            }
        }
    }

    /**
     * the watcher called on
     * getting watch while watching
     * my predecessor
     */
    
    private class LockWatcher implements Watcher {
        
        @Override
        public void process(WatchedEvent event) {
            // lets either become the leader or watch the new/updated node
            //LOG.debug("Watcher fired on path: {} state: {} type {}", event.getPath(), event.getState(), event.getType());
            try {
               lock();
            } catch (Exception e) {
                //LOG.warn("Failed to acquire lock", e);
            }
        }
    }

    /**
     * a zoookeeper operation that is mainly responsible
     * for all the magic required for locking.
     */
    private class LockZooKeeperOperation implements ZooKeeperOperation {

        /**
         * find if we have been created earlier if not create our node
         *
         * @param prefix    the prefix node
         * @param zookeeper teh zookeeper client
         * @param dir       the dir paretn
         * @param lockInfo 
         */
        private void createChildLock(String prefix, ZooKeeper zookeeper, String dir, byte[] lockInfo, CreateMode mode)
                throws KeeperException, InterruptedException
        {
               String lockPath = dir + ZooKeeperUtil.getLockPathByLockName(prefix);
               id = createZnode(lockPath, zookeeper, data, mode);
               //LOG.debug("Created id: {}", id);
        }

        /**
         * the command that is run and retried for actually
         * obtaining the lock
         *
         * @return if the command was successful or not
         * @throws InterruptedException 
         * @throws KeeperException 
         */
        public boolean execute() throws KeeperException, InterruptedException{
            do {
                if (id == null) {
                    //long sessionId = zookeeper.getSessionId();
                    //String prefix = "x-" + sessionId + "-";
                    //String prefix = "x-"
                    String prefix = lockType.getValue() + "-";
                    // lets try look up the current ID if we failed 
                    // in the middle of creating the znode
                    createChildLock(prefix, zookeeper, dir, data, CreateMode.EPHEMERAL_SEQUENTIAL);
                    idName = new ZNodeName(id);
                }
                if (id != null) {
                    List<String> names = zookeeper.getChildren(dir, false);
                    names.remove(PRIVATE_LOCK_NAME);
                    if (names.isEmpty()) {
                        //LOG.warn("No children in: {} when we've just created one! Lets recreate it...", dir);
                        // lets force the recreation of the id
                        // id = null;
                    } else {
                      
                        // lets sort them explicitly (though they do seem to come back in order ususally :)
                        SortedSet<ZNodeName> sortedNames = new TreeSet<>();
                        for (String name : names) {
                          if (name.equals(PRIVATE_LOCK_NAME))
                            continue;
                          sortedNames.add(new ZNodeName(dir + "/" + name));
                        }
                        ownerId = sortedNames.first().getName();
                        SortedSet<ZNodeName> lessThanMe = sortedNames.headSet(idName);
                        if (!lessThanMe.isEmpty()) {
                            ZNodeName lastChildName = lessThanMe.last();
                            lastChildId = lastChildName.getName();
                            /*if (LOG.isDebugEnabled()) {
                                LOG.debug("watching less than me node: {}", lastChildId);
                            }*/
                            Stat stat = zookeeper.exists(lastChildId, new LockWatcher());
                            if (stat != null) {
                                return Boolean.FALSE;
                            } else {
                                //LOG.warn("Could not find the stats for less than me: {}", lastChildName.getName());
                            }
                        } else {
                            if (isOwner() && isPrivateLockAvailable()) {
                                if (callback != null) {
                                  hasPrivateLock = true;
                                  byte[] privateLockMsg = ZooKeeperUtil.getDataBytes("Private lock is with " + id);
                                  createZnode(PRIVATE_LOCK_PATH, zookeeper, privateLockMsg, CreateMode.EPHEMERAL);
                                  callback.setEntiyId(dir);
                                  callback.setLockId(id);
                                  callback.lockAcquired();
                                }
                                return Boolean.TRUE;
                            }
                            else {
                              return Boolean.FALSE;
                            }
                        }
                    }
                }
            }
            while (id == null);
            return Boolean.FALSE;
        }

        private boolean isPrivateLockAvailable() throws KeeperException, InterruptedException
        {

          return !isPathExists(PRIVATE_LOCK_PATH);
        }
    }

    /**
     * Attempts to acquire the exclusive write lock returning whether or not it was
     * acquired. Note that the exclusive lock may be acquired some time later after
     * this method has been invoked due to the current lock owner going away.
     */
    public synchronized boolean lock() throws KeeperException, InterruptedException {
        if (isClosed()) {
            return false;
        }
        ensurePathExists(dir, data);

        return (Boolean) retryOperation(zop);
    }

    /**
     * return the data stored in lock.
     * if parent is created for the first time, parent contains data data provoded to first-born 
     *
     * @return data stored in lock.
     */
    public byte[] getData() {
        return data;
    }
    
    /**
     * return the parent dir for lock
     *
     * @return the parent dir used for locks.
     */
    public String getDir() {
        return dir;
    }

    /**
     * Returns true if this node is the owner of the
     * lock (or the leader)
     */
    public boolean isOwner() {
        return id != null && ownerId != null && id.equals(ownerId);
    }

    /**
     * return the id for this lock
     *
     * @return the id for this lock
     */
    public String getId() {
        return this.id;
    }
}

