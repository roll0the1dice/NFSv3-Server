package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class WRITE3resok {
  public enum StableHow {
    UNSTABLE,
    DATA_SYNC,
    FILE_SYNC;
  };

  WccData fileWcc;
  int count;
  StableHow committed;
  long verifier;

  public void serialize(ByteBuffer buffer) {
    fileWcc.serialize(buffer);
    buffer.putInt(count);
    buffer.putInt(committed.ordinal());
    buffer.putLong(verifier);
  }

  public int getSerializedSize() {
    return fileWcc.getSerializedSize() + 16;
  }
}
