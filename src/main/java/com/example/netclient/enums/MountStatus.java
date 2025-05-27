package com.example.netclient.enums;

import java.util.HashMap;
import java.util.Map;

public enum MountStatus {
  //// MOUNT Status Codes
  MNT_OK(0, "MNT OK"),
  MNT_ERR_PERM(1, "MNT err"),
  MNT_ERR_NOENT(2, "Mount Error NoContent"),
  MNT_ERR_IO(5, "Mount Error IO"),
  MNT_ERR_ACCES(13, "Mount Error Access"),
  MNT_ERR_NOTDIR(20, "Mount Error NotDir"),
  MNT_ERR_INVAL(22, "Mount Error Invalid"),
  MNT_ERR_NAMETOOLONG(63, "Mount Error NameToolong"),
  MNT_ERR_NOTSUPP(10004,"Mount Error NoSupport"),
  MNT_ERR_SERVERFAULT(10006,"Mount Error ServerFault");

  private int code;
  private String desc;

  MountStatus(int code, String desc) {
    this.code = code;
    this.desc = desc;
  }

  private static Map<Integer, MountStatus> map = new HashMap<Integer, MountStatus>();

  static {
    for (MountStatus constant : MountStatus.values()) {
      map.put(constant.code, constant);
    }
  }

  public static MountStatus fromCode(int code) {
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
