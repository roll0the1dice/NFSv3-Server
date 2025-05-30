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
public class PostOpFileHandle3 implements SerializablePayload {
  private int handleFollows;
  private NfsFileHandle3 nfsFileHandle;

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.putInt(handleFollows);
    nfsFileHandle.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    int t = handleFollows > 0 ? nfsFileHandle.getSerializedSize() : 0;
    return 4 + t;
  }

  @Override
  public void serialize(Buffer buffer) {
    buffer.appendInt(handleFollows);
    nfsFileHandle.serialize(buffer);
  }
}
