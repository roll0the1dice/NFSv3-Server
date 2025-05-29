package com.example.netclient.model;

import com.example.netclient.enums.Nfs3Constant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class NfsTime3 {
  int seconds;
  int nseconds;

  public void serialize(ByteBuffer buffer) {
    buffer.putInt(seconds);
    buffer.putInt(nseconds);
  }

  public static int getSerializedSize() {
    return 8;
  }
}
