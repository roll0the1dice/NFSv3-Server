package com.example.netclient.model;

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

  WccData fileWcc;
  int count;
  StableHow committed;
  long verifier;

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
}
