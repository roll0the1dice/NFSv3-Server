package com.example.netclient.httpclient;

import java.util.Map;

public interface AwsSigner {
  String calculateAuthorization(String httpVerb, String resourceUrl, Map<String, String> headers, byte[] payload);
  String getAmzDate();
  byte[] getChunkMetaData(byte[] chunkData);
  byte[] getFinalSignedChunkData();
}
