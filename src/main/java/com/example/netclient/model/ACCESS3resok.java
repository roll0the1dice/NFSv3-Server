package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class ACCESS3resok implements SerializablePayload {
  private PostOpAttr objAttributes;
  private int accessFlags;

  @Override
  public void serialize(ByteBuffer buffer) {
    objAttributes.serialize(buffer);
    buffer.putInt(accessFlags);
  }

  @Override
  public int getSerializedSize() {

    return 4 + objAttributes.getSerializedSize();
  }
}
