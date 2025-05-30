package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;
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

  @Override
  public void serialize(Buffer buffer) {
    objAttributes.serialize(buffer);
    buffer.appendInt(linkmax);
    buffer.appendInt(nameMax);
    buffer.appendInt(noTrunc);
    buffer.appendInt(chownRestricted);
    buffer.appendInt(caseInsensitive);
    buffer.appendInt(casePreserving);
  }
}
