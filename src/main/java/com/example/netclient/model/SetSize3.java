package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SetSize3 implements SerializablePayload, DeserializablePayload{
  public int setIt;
  public long size;

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.putInt(setIt);
    if (setIt > 0) {
      buffer.putLong(size);
    }
  }

  @Override
  public int getSerializedSize() {
    return 4 + (setIt > 0 ? 8 : 0);
  }

  @Override
  public void serialize(Buffer buffer) {
    buffer.appendInt(setIt);
    if (setIt > 0) {
      buffer.appendLong(size);
    }
  }

  @Override
  public void deserialize(Buffer buffer, int startingOffset) {
    int index = startingOffset;
    setIt = buffer.getInt(index);
    if (setIt == 1) {
      size = buffer.getInt(index + 4);
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
