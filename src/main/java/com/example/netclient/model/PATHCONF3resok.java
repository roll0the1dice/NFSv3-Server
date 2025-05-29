package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class PATHCONF3resok implements SerializablePayload {
  private PostOpAttr objAttributes;
  int linkmax;
  int nameMax;
  int noTrunc;
  int chownRestricted;
  int caseInsensitive;
  int casePreserving;

  @Override
  public void serialize(ByteBuffer buffer) {
    objAttributes.serialize(buffer);
    buffer.putInt(linkmax);
    buffer.putInt(nameMax);
    buffer.putInt(noTrunc);
    buffer.putInt(chownRestricted);
    buffer.putInt(caseInsensitive);
    buffer.putInt(casePreserving);
  }

  @Override
  public int getSerializedSize() {
    return objAttributes.getSerializedSize() + //
      24;
  }
}
