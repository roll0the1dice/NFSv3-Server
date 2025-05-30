package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class COMMIT3resok implements SerializablePayload {
  private WccData fileWcc;
  private long verifier;

  @Override
  public void serialize(ByteBuffer buffer) {
    fileWcc.serialize(buffer);
    buffer.putLong(verifier);
  }

  @Override
  public int getSerializedSize() {
    return fileWcc.getSerializedSize() + 8;
  }

  @Override
  public void serialize(Buffer buffer) {
    fileWcc.serialize(buffer);
    buffer.appendLong(verifier);
  }
}
