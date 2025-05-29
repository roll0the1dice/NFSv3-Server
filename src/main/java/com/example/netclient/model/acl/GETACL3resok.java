package com.example.netclient.model.acl;

import com.example.netclient.model.SerializablePayload;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.nio.ByteBuffer;


@Data
@AllArgsConstructor
@Builder
public class GETACL3resok implements SerializablePayload {
  @Override
  public void serialize(ByteBuffer buffer) {

  }

  @Override
  public int getSerializedSize() {
    return 0;
  }
}
