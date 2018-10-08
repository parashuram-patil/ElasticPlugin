package com.psp.plugin.configuration;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;

import com.fasterxml.jackson.core.type.TypeReference;
import com.psp.excception.plugin.PluginException;
import com.psp.response.ResponseCarrier;
import com.psp.util.jackson.ObjectMapperUtil;

public abstract class AbstractElasticPlugin extends BaseRestHandler {
  
  protected AbstractElasticPlugin(Settings settings, RestController controller, Client client)
  {
    super(settings, controller, client);
  }
  
  @Override
  protected void handleRequest(RestRequest request, RestChannel channel, Client client)
      throws Exception
  {
    try {
      Map<String, String> params = request.params();
      Method method = request.method();
      params.put("param_path", request.path());
      
      Map<String, Object> postData = new HashMap<>();
      
      if (method.equals(Method.POST) || method.equals(Method.PUT)) {
        BytesReference postBody = request.content();
        postData = ObjectMapperUtil.readValue(postBody.streamInput(),
            new TypeReference<Map<String, Object>>()
            {
            });
      }
      else if (method.equals(Method.GET)) {
        
      }
      else {
        throw new PluginException("Method Not Supported");
      }
      
      Map<String, Object> result = execute(postData, params);
      ResponseCarrier.successResponse(channel, result);
    }
    catch (Exception e) {
      ResponseCarrier.failedResponse(channel, e);
    }
    catch (Throwable e) {
      ResponseCarrier.failedResponse(channel, e);
    }
  }
  
  protected abstract Map<String, Object> execute(Map<String, Object> requestMap,
      Map<String, String> params) throws Throwable;
  
}
