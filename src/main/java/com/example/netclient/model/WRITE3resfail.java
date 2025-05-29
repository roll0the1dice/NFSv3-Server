package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class WRITE3resfail implements SerializablePayload {
  WccData fileWcc;

  @Override
  public void serialize(ByteBuffer buffer) {
    fileWcc.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return fileWcc.getSerializedSize();
  }
}
