package com.example.netclient.model.acl;

import com.example.netclient.model.PostOpAttr;
import com.example.netclient.model.SerializablePayload;

import java.nio.ByteBuffer;

public class SETACL3resok implements SerializablePayload {
  private PostOpAttr objAttributes;

  @Override
  public void serialize(ByteBuffer buffer) {
    objAttributes.serialize(buffer);
  }

  @Override
  public int getSerializedSize() {
    return objAttributes.getSerializedSize();
  }
}
