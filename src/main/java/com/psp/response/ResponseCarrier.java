package com.psp.response;

import java.io.IOException;

import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;

import com.psp.util.jackson.ObjectMapperUtil;

public class ResponseCarrier {
  
  public static void successResponse(RestChannel channel, Object response) throws IOException
  {
    channel.sendResponse(
        new BytesRestResponse(RestStatus.OK, ObjectMapperUtil.writeValueAsString(response)));
    
  }
  
  public static void failedResponse(RestChannel channel, Throwable e) throws IOException
  {
    RestStatus status = RestStatus.INTERNAL_SERVER_ERROR;
    channel.sendResponse(
        new BytesRestResponse(status, ObjectMapperUtil.writeValueAsString(e)));
  }  
}