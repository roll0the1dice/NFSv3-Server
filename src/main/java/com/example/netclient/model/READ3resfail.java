package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class READ3resfail implements SerializablePayload {
  PostOpAttr fileAttributes;

  @Override
  public void serialize(ByteBuffer buffer) {
    fileAttributes.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return fileAttributes.getSerializedSize();
  }
}
