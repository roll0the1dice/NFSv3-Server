package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class READDIRPLUS3resfail {
  int dirPresentFlag;
  FAttr3 dirAttributes;

  public void serialize(ByteBuffer buffer) {
    buffer.putInt(dirPresentFlag);
    if (dirPresentFlag != 0 && dirAttributes != null) {
      dirAttributes.serialize(buffer);
    }
  }

  public int getSerializedSize() {
    // obj Present Flag
    int t = dirPresentFlag != 0 ? FAttr3.getSerializedSize() : 4;
    return 4 + t;
  }
}
