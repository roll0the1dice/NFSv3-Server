package com.example.netclient.enums;

import java.util.HashMap;
import java.util.Map;

public enum MountProcedure {
  //// MOUNT Procedure Numbers
  MOUNTPROC_NULL(0, "Mount process null"),
  MOUNTPROC_MNT(1, "Mount process mnt"),
  MOUNTPROC_DUMP(2, "Mount process dump"),
  MOUNTPROC_UMNT(3, "Mount process umnt"),
  MOUNTPROC_UMNTALL(4, "Mount process umntall"),
  MOUNTPROC_EXPORT(5, "Mount process export");


  private final int code;
  private String desc;

  MountProcedure(int code, String desc) {
    this.code = code;
    this.desc = desc;
  }

  private static Map<Integer, MountProcedure> map = new HashMap<Integer, MountProcedure>();

  static {
    for (MountProcedure constant : MountProcedure.values()) {
      map.put(constant.code, constant);
    }
  }

  public static MountProcedure fromCode(int code) {
    return map.get(code);
  }

  public int getCode() {
    return code;
  }

  public String getDesc() {
    return desc;
  }

  @Override
  public String toString() {
    return desc;
  }

}

