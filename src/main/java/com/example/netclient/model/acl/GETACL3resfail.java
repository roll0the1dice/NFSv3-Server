package com.example.netclient.model.acl;

import com.example.netclient.model.PostOpAttr;
import com.example.netclient.model.SerializablePayload;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class GETACL3resfail implements SerializablePayload {
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
