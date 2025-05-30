package com.example.netclient.model.acl;

import com.example.netclient.model.PostOpAttr;
import com.example.netclient.model.SerializablePayload;
import io.vertx.core.buffer.Buffer;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class SETACL3resfail implements SerializablePayload {
  private PostOpAttr objAttributes;

  @Override
  public void serialize(ByteBuffer buffer) {
    objAttributes.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return objAttributes.getSerializedSize();
  }

  @Override
  public void serialize(Buffer buffer) {

  }
}
