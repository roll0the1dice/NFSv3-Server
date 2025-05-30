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
public class ACCESS3resfail implements SerializablePayload {
  private PostOpAttr dirAttributes;

  @Override
  public void serialize(ByteBuffer buffer) {
    dirAttributes.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return dirAttributes.getSerializedSize();
  }

  @Override
  public void serialize(Buffer buffer) {
    dirAttributes.serialize(buffer);
  }
}
