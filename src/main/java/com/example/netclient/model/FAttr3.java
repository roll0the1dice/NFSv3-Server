package com.example.netclient.model;

import com.example.netclient.enums.Nfs3Constant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.nio.ByteBuffer;

@Data
@AllArgsConstructor
@Builder
public class FAttr3 implements SerializablePayload {
  private int type;
  private int mode;
  private int nlink;
  private int uid;
  private int gid;
  private long size;
  private long used;
  private long rdev;
  private int fsidMajor;
  private int fsidMinor;
  private long fileid;
  private int atimeSeconds;
  private int atimeNseconds;
  private int mtimeSeconds;
  private int mtimeNseconds;
  private int ctimeSeconds;
  private int ctimeNseconds;

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.putInt(type);
    buffer.putInt(mode);
    buffer.putInt(nlink);
    buffer.putInt(uid);
    buffer.putInt(gid);
    buffer.putLong(size);
    buffer.putLong(used);
    buffer.putLong(rdev);
    buffer.putInt(fsidMajor);
    buffer.putInt(fsidMinor);
    buffer.putLong(fileid);
    buffer.putInt(atimeSeconds);
    buffer.putInt(atimeNseconds);
    buffer.putInt(mtimeSeconds);
    buffer.putInt(mtimeNseconds);
    buffer.putInt(ctimeSeconds);
    buffer.putInt(ctimeNseconds);
  }

  @Override
  public int getSerializedSize() {
    return Nfs3Constant.FILE_ATTR_SIZE;
  }

}
