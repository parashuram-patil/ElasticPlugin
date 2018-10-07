package com.cs.excception.zookeeper;

public class ZookeeperInitializationException extends Exception {
  
  private static final long serialVersionUID = 1L;
  
  public ZookeeperInitializationException()
  {
    super();
  }
  
  public ZookeeperInitializationException(String exceptionMessage)
  {
    super(exceptionMessage);
  }
  
  public ZookeeperInitializationException(Exception e)
  {
    super(e);
  }
  
}