package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class WRITE3resok implements SerializablePayload {
  public enum StableHow {
    UNSTABLE,
    DATA_SYNC,
    FILE_SYNC;
  };

  private WccData fileWcc;
  private int count;
  private StableHow committed;
  private long verifier;

  @Override
  public void serialize(ByteBuffer buffer) {
    fileWcc.serialize(buffer);
    buffer.putInt(count);
    buffer.putInt(committed.ordinal());
    buffer.putLong(verifier);
  }

  @Override
  public int getSerializedSize() {
    return fileWcc.getSerializedSize() + 16;
  }

  @Override
  public void serialize(Buffer buffer) {
    fileWcc.serialize(buffer);
    buffer.appendInt(count);
    buffer.appendInt(committed.ordinal());
    buffer.appendLong(verifier);
  }
}
