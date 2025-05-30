package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class LOOKUP3resok implements SerializablePayload {
  private int objHandlerLength; // object handle length
  private byte[] objectHandleData; // object handle data
  private PostOpAttr objAttributes;
  private PostOpAttr dirAttributes;

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.putInt(objHandlerLength);
    buffer.put(objectHandleData);
    int paddingBytes = (objectHandleData.length + 4 - 1) / 4 * 4 - objectHandleData.length;
    for (int i = 0; i < paddingBytes; i++) {
      buffer.put((byte) 0);
    }
    objAttributes.serialize(buffer);
    dirAttributes.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return 4 + // object handle length
      (objHandlerLength + 4 - 1) / 4 * 4 + // object handle
      objAttributes.getSerializedSize() + // obj
      dirAttributes.getSerializedSize(); // dir
  }

  @Override
  public void serialize(Buffer buffer) {
    buffer.appendInt(objHandlerLength);
    buffer.appendBytes(objectHandleData);
    int paddingBytes = (objectHandleData.length + 4 - 1) / 4 * 4 - objectHandleData.length;
    for (int i = 0; i < paddingBytes; i++) buffer.appendByte((byte) 0);
    objAttributes.serialize(buffer);
    dirAttributes.serialize(buffer);
  }

}
