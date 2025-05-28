package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class ACCESS3resok {
  int objPresentFlag;
  FAttr3 objAttributes;
  int accessFlags;

  public void serialize(ByteBuffer buffer) {
    buffer.putInt(objPresentFlag);
    if (objPresentFlag != 0 && objAttributes != null) {
      objAttributes.serialize(buffer);
    } else {
      buffer.putInt(0);
    }
    buffer.putInt(accessFlags);
  }

  public int getSerializedSize() {
    // obj Present Flag
    int t = objPresentFlag != 0 ? FAttr3.getSerializedSize() : 4;
    return 4 + t + 4;
  }
}
