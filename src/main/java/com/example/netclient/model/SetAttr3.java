package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SetAttr3 implements DeserializablePayload{
  public SetMode3 mode;
  public SetUid3 uid;
  public SetGid3 gid;
  public SetSize3 size;
  public int atime;
  public int mtime;

  @Override
  public void deserialize(Buffer buffer, int startingOffset) {
    int index = startingOffset;
    mode = new SetMode3();
    mode.deserialize(buffer, index);
    index += mode.getDeserializedSize();
    uid = new SetUid3();
    uid.deserialize(buffer, index);
    index += uid.getDeserializedSize();
    gid = new SetGid3();
    gid.deserialize(buffer, index);
    index += gid.getDeserializedSize();
    size = new SetSize3();
    size.deserialize(buffer, index);
    index += size.getDeserializedSize();
    atime = buffer.getInt(index);
    index += 4;
    mtime = buffer.getInt(index);
  }

  @Override
  public void deserialize() {

  }

  @Override
  public int getDeserializedSize() {
    return mode.getDeserializedSize() + uid.getDeserializedSize() + gid.getDeserializedSize() + size.getDeserializedSize() + 4 + 4;
  }
}
