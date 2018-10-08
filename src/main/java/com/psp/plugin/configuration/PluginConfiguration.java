package com.psp.plugin.configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.Plugin;

import com.psp.url.handler.RequestHandler;

public class PluginConfiguration extends Plugin {
  
  @Override
  public String description()
  {
    return "Elasticsearch Plugin";
  }
  
  @Override
  public String name()
  {
    return "elasticsearch-plugin";
  }
  
  @Override
  public Collection<Module> nodeModules()
  {
    List<Module> modules = new ArrayList<>();
    modules.add(new PluginModuleConfiguration());
    return modules;
  }
  
  public static class PluginModuleConfiguration extends AbstractModule {
    
    @Override
    protected void configure()
    {
      bind(RequestHandler.class).asEagerSingleton();
    }
  }
  
}