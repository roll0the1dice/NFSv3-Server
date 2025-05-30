package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class PostOpAttr implements SerializablePayload {
  private int attributesFollow; // present flag
  private FAttr3 attributes;

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.putInt(attributesFollow);
    if (attributesFollow != 0 && attributes != null) {
      attributes.serialize(buffer);
    }
  }

  @Override
  public int getSerializedSize() {
    // obj Present Flag
    int t = attributesFollow != 0 ? attributes.getSerializedSize() : 0;
    return 4 + t;
  }

  @Override
  public void serialize(Buffer buffer) {
    buffer.appendInt(attributesFollow);
    if (attributesFollow != 0 && attributes != null) {
      attributes.serialize(buffer);
    }
  }
}
