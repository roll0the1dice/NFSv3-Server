package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class GETATTR3resfail implements SerializablePayload {
  @Override
  public void serialize(ByteBuffer buffer) {

  }

  @Override
  public int getSerializedSize() {
    return 0;
  }

  @Override
  public void serialize(Buffer buffer) {

  }
}
