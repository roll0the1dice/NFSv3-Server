package com.example.netclient.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@Getter
@AllArgsConstructor
@ToString(of = "desc")
public enum MountProcedure implements BaseEnum {
  //// MOUNT Procedure Numbers
  MOUNTPROC_NULL(0, "Mount process null"),
  MOUNTPROC_MNT(1, "Mount process mnt"),
  MOUNTPROC_DUMP(2, "Mount process dump"),
  MOUNTPROC_UMNT(3, "Mount process umnt"),
  MOUNTPROC_UMNTALL(4, "Mount process umntall"),
  MOUNTPROC_EXPORT(5, "Mount process export");

  private final int code;
  private final String desc;
}

