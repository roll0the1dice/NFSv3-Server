package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;

import java.nio.ByteBuffer;

public interface SerializablePayload {
  void serialize(ByteBuffer buffer);
  int getSerializedSize();
  void serialize(Buffer buffer);
}
