package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class GETATTR3resok implements SerializablePayload {
  private FAttr3 objAttributes;

  @Override
  public void serialize(ByteBuffer buffer) {
    objAttributes.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return objAttributes.getSerializedSize();
  }

  @Override
  public void serialize(Buffer buffer) {
    objAttributes.serialize(buffer);
  }
}
