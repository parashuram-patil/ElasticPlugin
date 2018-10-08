package com.psp.url.handler;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import com.psp.util.zookeeper.InitializeZookeeper;

public class RequestHandler extends BaseRestHandler {

  @Inject
  public RequestHandler(Settings settings, RestController controller, Client client)
  {
    super(settings, controller, client);
    controller.registerHandler(Method.GET, "ping", this);    

    /******************************************* Zookeeper *******************************************/
    controller.registerHandler(Method.GET, "initializeZookeeper",
        new InitializeZookeeper(settings, controller, client));
  }
  
  @Override
  public void handleRequest(RestRequest request, RestChannel channel, Client client)
      throws Exception
	{
		try {
			if (request.path().contains("ping")) {
				RestResponse response = new BytesRestResponse(RestStatus.OK, "pong");
				channel.sendResponse(response);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
}
