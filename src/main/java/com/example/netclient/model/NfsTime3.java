package com.example.netclient.model;

import com.example.netclient.enums.Nfs3Constant;
import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class NfsTime3 implements SerializablePayload {
  private int seconds;
  private int nseconds;

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.putInt(seconds);
    buffer.putInt(nseconds);
  }

  @Override
  public int getSerializedSize() {
    return 8;
  }

  @Override
  public void serialize(Buffer buffer) {
    buffer.appendInt(seconds);
    buffer.appendInt(nseconds);
  }
}
