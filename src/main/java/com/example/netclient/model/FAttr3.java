package com.example.netclient.model;

import com.example.netclient.enums.Nfs3Constant;
import io.vertx.core.buffer.Buffer;
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

  @Override
  public void serialize(Buffer buffer) {
    buffer.appendInt(type);
    buffer.appendInt(mode);
    buffer.appendInt(nlink);
    buffer.appendInt(uid);
    buffer.appendInt(gid);
    buffer.appendLong(size);
    buffer.appendLong(used);
    buffer.appendLong(rdev);
    buffer.appendInt(fsidMajor);
    buffer.appendInt(fsidMinor);
    buffer.appendLong(fileid);
    buffer.appendInt(atimeSeconds);
    buffer.appendInt(atimeNseconds);
    buffer.appendInt(mtimeSeconds);
    buffer.appendInt(mtimeNseconds);
    buffer.appendInt(ctimeSeconds);
    buffer.appendInt(ctimeNseconds);
  }

}
