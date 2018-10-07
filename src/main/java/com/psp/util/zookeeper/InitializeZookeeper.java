package com.cs.util.zookeeper;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;

import com.cs.excception.zookeeper.ZookeeperInitializationException;
import com.cs.plugin.configuration.AbstractElasticPlugin;

public class InitializeZookeeper extends AbstractElasticPlugin {
  
  private static final String TEMP_PATH                                 = "tempPath";
  
  private static final String CONNECTED_TO_ZK                           = "Connected to Zookeeper";
  private static final String UNABLE_TO_CONNECT_ZK                      = "Unable to connect Zookeeper";
  private static final String UNABLE_TO_CONNECT_ANY_ZK_SERVER           = "Unable to connect any Zookeeper server";
  private static final String ERROR_IN_READING_ZK_PROPERTIES_FILE       = "Error in reading Zookeepers properties file";
  
  private static final String ZK_PROPERTIES_FILENAME                    = "zookeeper.properties";
  
  private static final String ZK_SERVERS_PROPERTY                       = "zookeeper.servers";
  private static final String ZK_FOREGROUND_LOCK_PREFIX_PROPERTY        = "zookeeper.foregound.lock.prefix";
  private static final String ZK_BACKGROUND_LOCK_PREFIX_PROPERTY        = "zookeeper.backgound.lock.prefix";
  private static final String ZK_SESSION_TIMEOUT_PROPERTY               = "zookeeper.session.timeout";
  private static final String ZK_CONNECTION_RETRY_COUNT_PROPERTY        = "zookeeper.connection.retry";
  private static final String ZK_FOREGROUND_TASK_TIMEOUT_PROPERTY       = "zookeeper.foregound.task.timeout";
  private static final String ZK_BACKGROUND_TASK_TIMEOUT_PROPERTY       = "zookeeper.backgound.task.timeout";
  private static final String ZK_CONNECTION_RETRY_DELAY_PROPERTY        = "zookeeper.connection.delay";
  
  private static String       ZK_SERVERS_PROPERTY_VALUE                 = null;
  private static String       ZK_FOREGROUND_LOCK_PREFIX_PROPERTY_VALUE  = null;
  private static String       ZK_BACKGROUND_LOCK_PREFIX_PROPERTY_VALUE  = null;
  private static Integer      ZK_SESSION_TIMEOUT_PROPERTY_VALUE         = null;
  private static Integer      ZK_CONNECTION_RETRY_COUNT_PROPERTY_VALUE  = null;
  private static Long         ZK_FOREGROUND_TASK_TIMEOUT_PROPERTY_VALUE = null;
  private static Long         ZK_BACKGROUND_TASK_TIMEOUT_PROPERTY_VALUE = null;
  private static Long         ZK_CONNECTION_RETRY_DELAY_PROPERTY_VALUE  = null;
  
  static {
    try {
      readProperties();
      ZooKeeper zk = ZooKeeperUtil.connect();
      connectToServer(zk);
      States state = zk.getState();
      if (state != States.CONNECTED) {
        printLogMessage(UNABLE_TO_CONNECT_ZK);
        throw new ZookeeperInitializationException(UNABLE_TO_CONNECT_ZK);
      }
      else {
        printLogMessage(CONNECTED_TO_ZK);
        ZooKeeperUtil.setZookeeper(zk);
      }
    }
    catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }
  
  public InitializeZookeeper(Settings settings, RestController controller, Client client)
  {
    super(settings, controller, client);
  }
  
  private static void readProperties() throws ZookeeperInitializationException
  {
    Properties props = new Properties();
    InputStream inputStream = null;
    ClassLoader loader = null;
    try {
      loader = InitializeZookeeper.class.getClassLoader();
      inputStream = loader.getResourceAsStream(ZK_PROPERTIES_FILENAME);
      props.load(inputStream);
      setProperties(props);
    }
    catch (Exception e) {
      printLogMessage(ERROR_IN_READING_ZK_PROPERTIES_FILE);
      throw new ZookeeperInitializationException(e);
    }
    finally {
      printProperties();
      if (inputStream != null) {
        try {
          inputStream.close();
        }
        catch (Exception e) {
          printLogMessage(ERROR_IN_READING_ZK_PROPERTIES_FILE);
          throw new ZookeeperInitializationException(e);
        }
      }
    }
  }
  
  private static void setProperties(Properties props) throws ZookeeperInitializationException
  {
    ZK_SERVERS_PROPERTY_VALUE                 = props.getProperty(ZK_SERVERS_PROPERTY);
    ZK_FOREGROUND_LOCK_PREFIX_PROPERTY_VALUE  = props.getProperty(ZK_FOREGROUND_LOCK_PREFIX_PROPERTY);
    ZK_BACKGROUND_LOCK_PREFIX_PROPERTY_VALUE  = props.getProperty(ZK_BACKGROUND_LOCK_PREFIX_PROPERTY);
    ZK_SESSION_TIMEOUT_PROPERTY_VALUE         = Integer.parseInt(props.getProperty(ZK_SESSION_TIMEOUT_PROPERTY));
    ZK_CONNECTION_RETRY_COUNT_PROPERTY_VALUE  = Integer.parseInt(props.getProperty(ZK_CONNECTION_RETRY_COUNT_PROPERTY));
    ZK_FOREGROUND_TASK_TIMEOUT_PROPERTY_VALUE = Long.parseLong(props.getProperty(ZK_FOREGROUND_TASK_TIMEOUT_PROPERTY));
    ZK_BACKGROUND_TASK_TIMEOUT_PROPERTY_VALUE = Long.parseLong(props.getProperty(ZK_BACKGROUND_TASK_TIMEOUT_PROPERTY));
    ZK_CONNECTION_RETRY_DELAY_PROPERTY_VALUE  = Long.parseLong(props.getProperty(ZK_CONNECTION_RETRY_DELAY_PROPERTY));
    
    if (isNull(ZK_SERVERS_PROPERTY_VALUE) || isNull(ZK_FOREGROUND_LOCK_PREFIX_PROPERTY_VALUE)
        || isNull(ZK_BACKGROUND_LOCK_PREFIX_PROPERTY_VALUE)
        || isNull(ZK_SESSION_TIMEOUT_PROPERTY_VALUE)
        || isNull(ZK_CONNECTION_RETRY_COUNT_PROPERTY_VALUE)
        || isNull(ZK_FOREGROUND_TASK_TIMEOUT_PROPERTY_VALUE)
        || isNull(ZK_BACKGROUND_TASK_TIMEOUT_PROPERTY_VALUE)
        || isNull(ZK_CONNECTION_RETRY_DELAY_PROPERTY_VALUE)) {
      
      throw new ZookeeperInitializationException(ERROR_IN_READING_ZK_PROPERTIES_FILE);
    }
  }

  private static void printProperties()
  {
    System.out.println("----------------------------------Zookeeper Properties--------------------------------------------");
    System.out.println("       " + ZK_SERVERS_PROPERTY + "                = " + ZK_SERVERS_PROPERTY_VALUE + "     ");
    System.out.println("       " + ZK_SESSION_TIMEOUT_PROPERTY + "        = " + ZK_SESSION_TIMEOUT_PROPERTY_VALUE + "     ");
    System.out.println("       " + ZK_CONNECTION_RETRY_COUNT_PROPERTY + "       = " + ZK_CONNECTION_RETRY_COUNT_PROPERTY_VALUE + "     ");
    System.out.println("       " + ZK_CONNECTION_RETRY_DELAY_PROPERTY + "       = " + ZK_CONNECTION_RETRY_DELAY_PROPERTY_VALUE + "     ");
    System.out.println("       " + ZK_FOREGROUND_LOCK_PREFIX_PROPERTY + "  = " + ZK_FOREGROUND_LOCK_PREFIX_PROPERTY_VALUE + "     ");
    System.out.println("       " + ZK_BACKGROUND_LOCK_PREFIX_PROPERTY + "  = " + ZK_BACKGROUND_LOCK_PREFIX_PROPERTY_VALUE + "     ");
    System.out.println("       " + ZK_FOREGROUND_TASK_TIMEOUT_PROPERTY + " = " + ZK_FOREGROUND_TASK_TIMEOUT_PROPERTY_VALUE + "     ");
    System.out.println("       " + ZK_BACKGROUND_TASK_TIMEOUT_PROPERTY + " = " + ZK_BACKGROUND_TASK_TIMEOUT_PROPERTY_VALUE + "     ");
    System.out.println("-------------------------------------------------------------------------------------------------");
  }

  private static void connectToServer(ZooKeeper zk)
  {
    String tempPath = ZooKeeperUtil.getLockPathByLockName(TEMP_PATH);
    try {
      zk.exists(tempPath, false);
    }
    catch (KeeperException | InterruptedException e) {
      printLogMessage(UNABLE_TO_CONNECT_ANY_ZK_SERVER);
      e.printStackTrace();
    }
  }
  
  private static void printLogMessage(String message)
  {
    System.out.println("----------------------------------------------------------------");
    System.out.println("      ___" + message + "___     ");
    System.out.println("----------------------------------------------------------------");
  }
  
  private static boolean isNull(String value)
  {
    boolean result = false;
    
    if(value == null)
      result = true;
    
    return result;
  }
  
  private static boolean isNull(Integer value)
  {
    boolean result = false;
    
    if(value == null)
      result = true;
    
    return result;
  }
  
  private static boolean isNull(Long value)
  {
    boolean result = false;
    
    if(value == null)
      result = true;
    
    return result;
  }
  
  static String getZkServers()
  {
    return ZK_SERVERS_PROPERTY_VALUE;
  }
  
  static String getZkForegoundLockPrefix()
  {
    return ZK_FOREGROUND_LOCK_PREFIX_PROPERTY_VALUE;
  }
  
  static String getZkBackgoundLockPrefix()
  {
    return ZK_BACKGROUND_LOCK_PREFIX_PROPERTY_VALUE;
  }
  
  static Integer getZkSessionTimeout()
  {
    return ZK_SESSION_TIMEOUT_PROPERTY_VALUE;
  }
  
  static Integer getZkConnectionRetryCount()
  {
    return ZK_CONNECTION_RETRY_COUNT_PROPERTY_VALUE;
  }

  static Long getZkForegroundTaskTimeout()
  {
    return ZK_FOREGROUND_TASK_TIMEOUT_PROPERTY_VALUE;
  }
  
  static Long getZkBackgroundTaskTimeout()
  {
    return ZK_BACKGROUND_TASK_TIMEOUT_PROPERTY_VALUE;
  }
  
  static Long getZkConnectionRetryDelay()
  {
    return ZK_CONNECTION_RETRY_DELAY_PROPERTY_VALUE;
  }
  
  @Override
  protected Map<String, Object> execute(Map<String, Object> requestMap, Map<String, String> params)
      throws Throwable
  {
    Map<String, Object> returnMap = new HashMap<>();
    
    ZooKeeper zookeeper = ZooKeeperUtil.getZookeeper();
    if(zookeeper != null) {
      returnMap.put("Success", zookeeper.getState());
    } else {
      returnMap.put("Failure", "Not Connected to Zookeeper");
    }
    
    return returnMap;
  }
}
