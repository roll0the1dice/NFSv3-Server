package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class CREATE3resfail implements SerializablePayload {
  private WccData dirWcc;

  @Override
  public void serialize(ByteBuffer buffer) {
    dirWcc.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return dirWcc.getSerializedSize();
  }
}
