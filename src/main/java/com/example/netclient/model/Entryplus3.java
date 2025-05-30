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
public class Entryplus3 implements SerializablePayload {
  private long fileid;
  private int fileNameLength;
  private byte[] fileName;
  private long cookie;
  private int nameAttrPresent;
  private FAttr3 nameAttr;
  private int nameHandlePresent;
  private int nameHandleLength;
  private byte[] nameHandle;
  private int nextEntryPresent;

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.putLong(fileid);
    buffer.putInt(fileNameLength);
    if (fileNameLength > 0 && fileName != null) {
      buffer.put(fileName);
    }
    int padding = ((fileNameLength + 3) & ~3) - fileNameLength;
    for (int i = 0; i < padding; i++) {
      buffer.put((byte) 0);
    }
    buffer.putLong(cookie);
    buffer.putInt(nameAttrPresent);
    if (nameAttrPresent != 0 && nameAttr != null) {
      nameAttr.serialize(buffer);
    }
    buffer.putInt(nameHandlePresent);
    buffer.putInt(nameHandleLength);
    buffer.put(nameHandle);
    padding = ((nameHandleLength + 3) & ~3) - nameHandleLength;
    for (int i = 0; i < padding; i++) {
      buffer.put((byte) 0);
    }
    buffer.putInt(nextEntryPresent);
  }

  @Override
  public int getSerializedSize() {
    // obj Present Flag
    return 8 + // fileid
      4 + // name length
      ((fileNameLength + 3) & ~3) + // name (padded to 4 bytes)
      8 + // cookie
      4 + // name_attributes present flag
      nameAttr.getSerializedSize() + // name_attributes
      4 + // handle present
      4 + // handle length
      ((nameHandleLength + 3) & ~3) + // handle data
      4;  // nextentry present flag
  }

  @Override
  public void serialize(Buffer buffer) {
    buffer.appendLong(fileid);
    buffer.appendInt(fileNameLength);
    if (fileNameLength > 0 && fileName != null) {
      buffer.appendBytes(fileName);
    }
    int padding = ((fileNameLength + 3) & ~3) - fileNameLength;
    for (int i = 0; i < padding; i++) buffer.appendByte((byte) 0);
    buffer.appendLong(cookie);
    buffer.appendInt(nameAttrPresent);
    if (nameAttrPresent != 0 && nameAttr != null) {
      nameAttr.serialize(buffer);
    }
    buffer.appendInt(nameHandlePresent);
    buffer.appendInt(nameHandleLength);
    buffer.appendBytes(nameHandle);
    padding = ((nameHandleLength + 3) & ~3) - nameHandleLength;
    for (int i = 0; i < padding; i++) buffer.appendByte((byte) 0);
    buffer.appendInt(nextEntryPresent);
  }

}
