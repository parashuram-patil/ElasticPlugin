package com.psp.excception.zookeeper;

public class InstanceLockException extends Exception {
  
  private static final long serialVersionUID = 1L;
  
  public InstanceLockException()
  {
    super();
  }
  
  public InstanceLockException(String exceptionMessage)
  {
    super(exceptionMessage);
  }
  
  public InstanceLockException(Exception e)
  {
    super(e);
  }
  
}
