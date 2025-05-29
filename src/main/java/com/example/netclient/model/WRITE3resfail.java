package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class WRITE3resfail {
  WccData fileWcc;

  public void serialize(ByteBuffer buffer) {
    fileWcc.serialize(buffer);
  }

  public int getSerializedSize() {
    return fileWcc.getSerializedSize();
  }
}
