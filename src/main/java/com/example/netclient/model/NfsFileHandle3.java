package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class NfsFileHandle3 implements SerializablePayload {
  private int handleOfLength;
  private byte[] fileHandle;

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.putInt(handleOfLength);
    if (handleOfLength > 0 && fileHandle != null) {
      buffer.put(fileHandle);
      int padding = (handleOfLength + 4 - 1) / 4 * 4 - handleOfLength;
      for (int i = 0; i < padding; i++) buffer.put((byte) 0);
    }
  }

  @Override
  public int getSerializedSize() {
    return 4 + //
      (handleOfLength + 4 - 1) / 4 * 4;
  }

  @Override
  public void serialize(Buffer buffer) {
    buffer.appendInt(handleOfLength);
    if (handleOfLength > 0 && fileHandle != null) {
      buffer.appendBytes(fileHandle);
      int padding = (handleOfLength + 4 - 1) / 4 * 4 - handleOfLength;
      for (int i = 0; i < padding; i++) buffer.appendByte((byte) 0);
    }
  }
}
