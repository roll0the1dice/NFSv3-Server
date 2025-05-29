package com.example.netclient.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class FSINFO3resfail {
  PostOpAttr objAttributes; // post_op_attr present flag

  public void serialize(ByteBuffer buffer) {
    objAttributes.serialize(buffer);
  }

  public int getSerializedSize() {
    return objAttributes.getSerializedSize();
  }
}
