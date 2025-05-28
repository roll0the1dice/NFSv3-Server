package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;


@Data
@AllArgsConstructor
@Builder
public class Entryplus3 {
  long fileid;
  int fileNameLength;
  byte[] fileName;
  long cookie;
  int nameAttrPresent;
  FAttr3 nameAttr;
  int nameHandlePresent;
  int nameHandleLength;
  byte[] nameHandle;
  int nextEntryPresent;

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

  public int getSerializedSize() {
    // obj Present Flag
    return 8 + // fileid
      4 + // name length
      ((fileNameLength + 3) & ~3) + // name (padded to 4 bytes)
      8 + // cookie
      4 + // name_attributes present flag
      FAttr3.getSerializedSize() + // name_attributes
      4 + // handle present
      4 + // handle length
      ((nameHandleLength + 3) & ~3) + // handle data
      4;  // nextentry present flag
  }

}
