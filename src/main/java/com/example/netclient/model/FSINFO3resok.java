package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class FSINFO3resok {
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

  public static int getSerializedSize() {
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
}
