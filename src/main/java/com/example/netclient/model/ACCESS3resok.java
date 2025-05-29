package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class ACCESS3resok {
  PostOpAttr objAttributes;
  int accessFlags;

  public void serialize(ByteBuffer buffer) {
    objAttributes.serialize(buffer);
    buffer.putInt(accessFlags);
  }

  public int getSerializedSize() {

    return 4 + objAttributes.getSerializedSize();
  }
}
