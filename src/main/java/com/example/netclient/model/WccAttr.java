package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

/**
 *    weak cache consistency, emphasizes the fact that this
 *    mechanism does not provide the strict server-client consistency
 *    that a cache consistency protocol would provide.
 */
@Data
@AllArgsConstructor
@Builder
public class WccAttr {
  long size;
  int mtimeSeconds;
  int mtimeNSeconds;
  int ctimeSeconds;
  int ctimeNSeconds;

  public void serialize(ByteBuffer buffer) {
    buffer.putLong(size);
    buffer.putInt(mtimeSeconds);
    buffer.putInt(mtimeNSeconds);
    buffer.putInt(ctimeSeconds);
    buffer.putInt(ctimeNSeconds);
  }

  public static int getSerializedSize() {
    return 24;
  }
}
