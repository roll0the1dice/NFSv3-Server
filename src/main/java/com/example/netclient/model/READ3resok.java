package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class READ3resok implements SerializablePayload {
  private PostOpAttr fileAttributes;
  private int count;
  private int eof;
  private int dataOfLength;
  private byte[] data;

  @Override
  public void serialize(ByteBuffer buffer) {
    fileAttributes.serialize(buffer);
    buffer.putInt(count);
    buffer.putInt(eof);
    buffer.putInt(dataOfLength);
    buffer.put(data);
    int padding = (dataOfLength + 4 - 1) / 4 * 4 - dataOfLength;
    for (int i = 0; i < padding; i++) buffer.put((byte) 0);
  }

  @Override
  public int getSerializedSize() {
    return 4 + // object handle length
      fileAttributes.getSerializedSize() + // rtmax
      4 + // count
      4 + // eof
      4 + // dataOfLength
      ((dataOfLength + 4 - 1) / 4 * 4); // data
  }
}
