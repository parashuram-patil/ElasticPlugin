package com.psp.util.zookeeper;

import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.zookeeper.ZooKeeper;

public class RecursiveDeleteZnodes {
  
  public static void main(String[] args) throws Exception
  {
    String path = "/";
    ZooKeeper zk = ZooKeeperUtil.getZookeeper();
    List<String> nodes = zk.getChildren(path, true);
    nodes.remove("zookeeper");
    //System.out.println(nodes.toString());
    TreeSet<String> ascNodes = new TreeSet<>();
    RecursiveDelete(nodes, path, 0, 0, zk, ascNodes);
    System.out.println("----------------------------");
    //System.out.println(ascNodes.toString());
    
    Iterator<String> iterator = ascNodes.descendingIterator();
    while(iterator.hasNext()) {
      String next = iterator.next();
      System.out.println(next);
      zk.delete(next, -1);
    }

  }
  
  
  static void RecursiveDelete(List<String> nodes, String parentNode, int index, int level, ZooKeeper zk, SortedSet<String> ascNodes) throws Exception
  {
    if (index == nodes.size())
      return;
    
    for (int i = 0; i < level; i++)
      System.out.print("\t");
    
    String node = nodes.get(index);
    node = node.startsWith("/") ? node : "/" + node;
    
    String path = (parentNode.equals("/") ? "" : parentNode) + node;
    List<String> children = zk.getChildren(path, true);
    if (children.size() == 0) {
      //System.out.println(path);
      System.out.println(node);
      ascNodes.add(path);
    }
    
    else if (children.size() > 0) {
      //System.out.println("[" + path + "]");
      System.out.println("[" + node + "]");
      ascNodes.add(path);

      RecursiveDelete(children, path, 0, level + 1, zk, ascNodes);
    }
    
    RecursiveDelete(nodes, parentNode, ++index, level, zk, ascNodes);
  }
}
