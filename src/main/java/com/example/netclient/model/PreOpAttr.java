package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class PreOpAttr {
  int attributesFollow; // present flag
  WccAttr attributes;

  public void serialize(ByteBuffer buffer) {
    buffer.putInt(attributesFollow);
    if (attributesFollow != 0 && attributes != null) {
      attributes.serialize(buffer);
    }
  }

  public int getSerializedSize() {
    // obj Present Flag
    int t = attributesFollow != 0 ? FAttr3.getSerializedSize() : 0;
    return 4 + t;
  }
}
