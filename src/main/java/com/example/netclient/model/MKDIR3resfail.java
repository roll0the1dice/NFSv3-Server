package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;

import java.nio.ByteBuffer;

public class MKDIR3resfail implements SerializablePayload {
  private WccData dirWcc;

  @Override
  public void serialize(ByteBuffer buffer) {
    dirWcc.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return dirWcc.getSerializedSize();
  }

  @Override
  public void serialize(Buffer buffer) {
    dirWcc.serialize(buffer);
  }
}
