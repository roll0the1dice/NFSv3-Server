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
public class SETATTR3resfail implements SerializablePayload {
  private WccData objWcc;

  @Override
  public void serialize(ByteBuffer buffer) {
    objWcc.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return objWcc.getSerializedSize();
  }

  @Override
  public void serialize(Buffer buffer) {
    objWcc.serialize(buffer);
  }
}
