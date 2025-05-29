package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class READDIRPLUS3resfail implements SerializablePayload {
  PostOpAttr dirAttributes;

  @Override
  public void serialize(ByteBuffer buffer) {
    dirAttributes.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return dirAttributes.getSerializedSize();
  }
}
