package com.cs.util.jackson;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

public class ObjectMapperUtil {
  
  private static final ObjectMapper mapper = new ObjectMapper();
  
  public static String writeValueAsString(Object instance) throws JsonProcessingException
  {
    return mapper.writeValueAsString(instance);
  }
  
  public static <T> T readValue(String json, Class<T> clazz) throws Exception
  {
    return mapper.readValue(json, clazz);
  }
  
  @SuppressWarnings("rawtypes")
  public static <T> T readValue(InputStream src, TypeReference valueTypeRef)
      throws IOException, JsonParseException, JsonMappingException
  {
    return mapper.readValue(src, valueTypeRef);
  }
  
  @SuppressWarnings("rawtypes")
  public static <T> T readValue(String src, TypeReference valueTypeRef)
      throws IOException, JsonParseException, JsonMappingException
  {
    return mapper.readValue(src, valueTypeRef);
  }
  
  public static <T> T readValue(InputStream src, Class<T> valueType)
      throws IOException, JsonParseException, JsonMappingException
  {
    return mapper.readValue(src, valueType);
  }
  
  @SuppressWarnings("rawtypes")
  public static <T> T readValue(Reader src, TypeReference valueTypeRef)
      throws IOException, JsonParseException, JsonMappingException
  {
    return mapper.readValue(src, valueTypeRef);
  }
  
  public static ObjectReader readerFor(Class<?> type)
  {
    return mapper.readerFor(type);
  }
  
  public static <T> T readValue(File src, Class<T> valueType)
      throws IOException, JsonParseException, JsonMappingException
  {
    return mapper.readValue(src, valueType);
  }
  
  public static <T> T convertValue(Object fromValue, Class<T> toValueType)
      throws IllegalArgumentException
  {
    return mapper.convertValue(fromValue, toValueType);
  }
  
  @SuppressWarnings("rawtypes")
  public static <T> T readValue(JsonParser json, TypeReference valueTypeRef)
      throws IOException, JsonParseException, JsonMappingException
  {
    return mapper.readValue(json, valueTypeRef);
  }
  
  @SuppressWarnings("rawtypes")
  public static <T> T readValue(File json, TypeReference valueTypeRef)
      throws IOException, JsonParseException, JsonMappingException
  {
    return mapper.readValue(json, valueTypeRef);
  }
}