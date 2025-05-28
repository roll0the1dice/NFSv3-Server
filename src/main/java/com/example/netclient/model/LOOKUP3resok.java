package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class LOOKUP3resok {
  int objHandlerLength; // object handle length
  byte[] objectHandleData; // object handle data
  int objPresentFlag;
  FAttr3 fAttr3;
  int dirPresentFlag;
  FAttr3 dirAttr3;

  public void serialize(ByteBuffer buffer) {
    buffer.putInt(objHandlerLength);
    buffer.put(objectHandleData);
    int paddingBytes = (objectHandleData.length + 4 - 1) / 4 * 4 - objectHandleData.length;
    for (int i = 0; i < paddingBytes; i++) {
      buffer.put((byte) 0);
    }
    buffer.putInt(objPresentFlag);
    if (objPresentFlag != 0 && fAttr3 != null) {
      fAttr3.serialize(buffer);
    } else {
      buffer.putInt(0);
    }
    buffer.putInt(dirPresentFlag);
    if (dirPresentFlag != 0 && dirAttr3 != null) {
      dirAttr3.serialize(buffer);
    } else {
      buffer.putInt(0);
    }
  }

  public int getSerializedSize() {
    return 4 + // object handle length
      (objHandlerLength + 4 - 1) / 4 * 4 + // rtmax
      4 + // obj Present Flag
      (objPresentFlag != 0 ? FAttr3.getSerializedSize() : 4) + // obj
      4 + // dir Present Flag
      (dirPresentFlag != 0 ? FAttr3.getSerializedSize() : 4); // dir
  }

}
