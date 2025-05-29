package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class READ3resfail {
  int filePresentFlag;
  FAttr3 fileAttributes;

  public void serialize(ByteBuffer buffer) {
    buffer.putInt(filePresentFlag);
    if (filePresentFlag != 0 && fileAttributes != null) {
      fileAttributes.serialize(buffer);
    }
  }

  public int getSerializedSize() {
    // obj Present Flag
    int t = filePresentFlag != 0 ? FAttr3.getSerializedSize() : 4;
    return 4 + t;
  }
}
