package com.psp.util.thread;

import java.util.HashMap;
import java.util.Map;

public class ThreadUtil {
  
  protected static Map<String, Object> monitorObjects = new HashMap<>();
  
  public static synchronized Object getMontiorObject(String id)
  {
    if (monitorObjects.get(id) == null) {
      monitorObjects.put(id, new Object());
    }
    return monitorObjects.get(id);
  }
  
}
