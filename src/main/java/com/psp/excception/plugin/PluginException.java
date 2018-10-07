package com.cs.excception.plugin;

public class PluginException extends Exception {
  
  private static final long serialVersionUID = 1L;
  
  public PluginException()
  {
    super();
  }
  
  public PluginException(String exceptionMessage)
  {
    super(exceptionMessage);
  }
  
  public PluginException(Exception e)
  {
    super(e);
  }
}