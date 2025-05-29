package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class WccData {
  PreOpAttr before;
  PostOpAttr after;

  public void serialize(ByteBuffer buffer) {
    before.serialize(buffer);
    after.serialize(buffer);
  }

  public int getSerializedSize() {
    return before.getSerializedSize() + after.getSerializedSize();
  }
}
