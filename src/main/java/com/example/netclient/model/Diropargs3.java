package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Diropargs3 implements DeserializablePayload  {
  public nfs_fh3 dir;
  public int filenameLength;
  public byte[] filename;

  @Override
  public void deserialize(Buffer buffer, int startingOffset) {
    int index = startingOffset;
    dir = new nfs_fh3();
    dir.deserialize(buffer, index);
    index += dir.getDeserializedSize();
    filenameLength = buffer.getInt(index);
    index += 4;
    filename = new byte[filenameLength];
    buffer.getBytes(index, index + filenameLength, filename); // Wrap the relevant part
  }

  @Override
  public void deserialize() {

  }

  @Override
  public int getDeserializedSize() {
    return dir.getDeserializedSize() + //
      4 + //
      (filenameLength + 4 - 1) / 4 * 4;
  }
}
