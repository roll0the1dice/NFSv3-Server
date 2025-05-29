package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@Data
@AllArgsConstructor
@Builder
public class SETATTR3args {
  NfsFileHandle3 object;
  SetAttr3 newAttributes;
  SAttrGuard3 guard;

  public static SETATTR3args deserialize(byte[] data) {
    ByteBuffer buffer = ByteBuffer.wrap(data);
    buffer.order(ByteOrder.BIG_ENDIAN);

    int handleOfLength = buffer.getInt();
    byte[] fileHandle = new byte[handleOfLength];
    buffer.get(fileHandle);
    int padding = (4 - (handleOfLength % 4)) % 4;
    buffer.position(buffer.position() + padding);
    NfsFileHandle3 nfsFileHandle3 = new NfsFileHandle3(handleOfLength, fileHandle);

    int setIt = buffer.getInt();
    int mode = setIt != 0 ? buffer.getInt() : 0;
    SetMode3 setMode3 = new SetMode3(setIt, mode);
    setIt = buffer.getInt();
    int uid = setIt != 0 ? buffer.getInt() : 0;
    SetUid3 uid3 = new SetUid3(setIt, uid);
    setIt = buffer.getInt();
    int gid = setIt != 0 ? buffer.getInt() : 0;
    SetGid3 gid3 = new SetGid3(setIt, gid);
    setIt = buffer.getInt();
    long size = setIt != 0 ? buffer.getLong() : 0;
    SetSize3 setSize3 = new SetSize3(setIt, size);
    setIt = buffer.getInt();
    SetAtime setAtime = new SetAtime(setIt, 0, 0);
    setIt = buffer.getInt();
    SetMtime setMtime = new SetMtime(setIt, 0, 0);
    SetAttr3 setAttr3 = new SetAttr3(setMode3, uid3, gid3, setSize3, setAtime, setMtime);

    int check = buffer.getInt();
    int seconds = check != 0 ? buffer.getInt() : 0;
    int Nseconds = check != 0 ? buffer.getInt() : 0;
    NfsTime3 nfsTime3 = new NfsTime3(seconds, Nseconds);
    SAttrGuard3 guard3 = new SAttrGuard3(nfsTime3);

    return new SETATTR3args(nfsFileHandle3, setAttr3, guard3);
  }
}
