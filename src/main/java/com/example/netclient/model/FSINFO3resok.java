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
public class FSINFO3resok implements SerializablePayload {
  private int post_op_attr;
  private int rtmax;
  private int rtpref;
  private int rtmult;
  private int wtmax;
  private int wtpref;
  private int wtmult;
  private int dtpref;
  private long maxFilesize;
  private int seconds;
  private int nseconds;
  private int extraField; // extra field

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.putInt(post_op_attr);
    buffer.putInt(rtmax);
    buffer.putInt(rtpref);
    buffer.putInt(rtmult);
    buffer.putInt(wtmax);
    buffer.putInt(wtpref);
    buffer.putInt(wtmult);
    buffer.putInt(dtpref);
    buffer.putLong(maxFilesize);
    buffer.putInt(seconds);
    buffer.putInt(nseconds);
    buffer.putInt(extraField);
  }

  @Override
  public int getSerializedSize() {
    return 4 + // post_op_attr
      4 + // rtmax
      4 + // rtpref
      4 + // rtmult
      4 + // wtmax
      4 + // wtpref
      4 + // wtmult
      4 + // dtpref
      8 + // maxFilesize
      8 + // timeDelta
      4;  // extraField
  }

  @Override
  public void serialize(Buffer buffer) {
    buffer.appendInt(post_op_attr);
    buffer.appendInt(rtmax);
    buffer.appendInt(rtpref);
    buffer.appendInt(rtmult);
    buffer.appendInt(wtmax);
    buffer.appendInt(wtpref);
    buffer.appendInt(wtmult);
    buffer.appendInt(dtpref);
    buffer.appendLong(maxFilesize);
    buffer.appendInt(seconds);
    buffer.appendInt(nseconds);
    buffer.appendInt(extraField);
  }
}
