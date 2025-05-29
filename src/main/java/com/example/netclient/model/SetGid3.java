package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@Data
@AllArgsConstructor
@Builder
public class SetGid3 implements SerializablePayload {
  public int setIt;
  public int gid;

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.putInt(setIt);
    if (setIt > 0) {
      buffer.putInt(gid);
    }
  }

  @Override
  public int getSerializedSize() {
    return 4 + (setIt > 0 ? 4 : 0);
  }

}
