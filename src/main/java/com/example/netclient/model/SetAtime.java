package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class SetAtime implements SerializablePayload {
  public int setIt;
  public int atimeSeconds;
  public int atimeNSeconds;

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.putInt(setIt);
    if (setIt > 0) {
      buffer.putInt(atimeSeconds);
      buffer.putInt(atimeNSeconds);
    }
  }

  @Override
  public int getSerializedSize() {
    return 4 + (setIt > 0 ? 8 : 0);
  }

  @Override
  public void serialize(Buffer buffer) {

  }
}
