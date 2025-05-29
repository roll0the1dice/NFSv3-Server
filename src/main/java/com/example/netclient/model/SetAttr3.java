package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@Data
@AllArgsConstructor
@Builder
public class SetAttr3 {
  private SetMode3 mode;
  private SetUid3 uid;
  private SetGid3 gid;
  private SetSize3 size;
  private SetAtime atime;
  private SetMtime mtime;
}
