package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class nfs_fh3 implements DeserializablePayload {
  public int handleOfLength;
  public byte[] fileHandle;


  @Override
  public void deserialize(Buffer buffer, int startingOffset) {
    int index = startingOffset;
    handleOfLength = buffer.getInt(index);
    index += 4;
    fileHandle = new byte[handleOfLength];
    buffer.getBytes(index, index + handleOfLength, fileHandle);
    int padding = (handleOfLength + 4 - 1) / 4 * 4 - handleOfLength;
    for (int i = 0; i < padding; i++) index++;
  }

  @Override
  public void deserialize() {

  }

  @Override
  public int getDeserializedSize() {
    return 4 + //
      (handleOfLength + 4 - 1) / 4 * 4;
  }
}
