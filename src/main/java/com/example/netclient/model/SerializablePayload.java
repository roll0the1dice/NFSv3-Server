package com.example.netclient.model;

import java.nio.ByteBuffer;

public interface SerializablePayload {
  void serialize(ByteBuffer buffer);
  int getSerializedSize();
}
