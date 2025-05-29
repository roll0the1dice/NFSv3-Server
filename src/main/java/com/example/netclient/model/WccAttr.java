package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 *    weak cache consistency, emphasizes the fact that this
 *    mechanism does not provide the strict server-client consistency
 *    that a cache consistency protocol would provide.
 */
@Data
@AllArgsConstructor
@Builder
public class WccAttr implements SerializablePayload {
  long size;
  int mtimeSeconds;
  int mtimeNSeconds;
  int ctimeSeconds;
  int ctimeNSeconds;

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.putLong(size);
    buffer.putInt(mtimeSeconds);
    buffer.putInt(mtimeNSeconds);
    buffer.putInt(ctimeSeconds);
    buffer.putInt(ctimeNSeconds);
  }

  @Override
  public int getSerializedSize() {
    return 24;
  }
}
