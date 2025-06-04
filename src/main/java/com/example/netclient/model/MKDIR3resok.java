package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;


@Data
@AllArgsConstructor
@Builder
public class MKDIR3resok implements SerializablePayload{
  private PostOpFileHandle3 obj;
  private PostOpAttr objAttributes;
  private WccData dirWcc;

  @Override
  public void serialize(ByteBuffer buffer) {
    obj.serialize(buffer);
    objAttributes.serialize(buffer);
    dirWcc.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return obj.getSerializedSize() + objAttributes.getSerializedSize() + dirWcc.getSerializedSize();
  }

  @Override
  public void serialize(Buffer buffer) {
    obj.serialize(buffer);
    objAttributes.serialize(buffer);
    dirWcc.serialize(buffer);
  }
}
