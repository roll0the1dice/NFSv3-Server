package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SetGid3 implements SerializablePayload, DeserializablePayload {
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

  @Override
  public void serialize(Buffer buffer) {
    buffer.appendInt(setIt);
    if (setIt > 0) {
      buffer.appendInt(gid);
    }
  }

  @Override
  public void deserialize(Buffer buffer, int startingOffset) {
    int index = startingOffset;
    setIt = buffer.getInt(index);
    if (setIt == 1) {
      gid = buffer.getInt(index + 4);
    }
  }

  @Override
  public void deserialize() {

  }

  @Override
  public int getDeserializedSize() {
    return 4 + //
      (setIt == 1 ? 4 : 0);
  }
}
