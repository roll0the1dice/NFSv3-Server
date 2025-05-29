package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class ACCESS3resfail {
  private PostOpAttr dirAttributes;

  public void serialize(ByteBuffer buffer) {
    dirAttributes.serialize(buffer);
  }

  public int getSerializedSize() {
    return dirAttributes.getSerializedSize();
  }
}
