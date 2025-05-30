package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class READ3resfail implements SerializablePayload {
  private PostOpAttr fileAttributes;

  @Override
  public void serialize(ByteBuffer buffer) {
    fileAttributes.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return fileAttributes.getSerializedSize();
  }

  @Override
  public void serialize(Buffer buffer) {
    fileAttributes.serialize(buffer);
  }
}
