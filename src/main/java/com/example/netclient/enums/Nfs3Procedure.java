package com.example.netclient.enums;

import java.util.HashMap;
import java.util.Map;

public enum Nfs3Procedure {
  NFSPROC_NULL(0, "NFSPROC_NULL"),
  NFSPROC_GETATTR(1, "NFSPROC_GETATTR"),
  NFSPROC_SETATTR(2, "NFSPROC_SETATTR"),
  NFSPROC_LOOKUP(3, "NFSPROC_LOOKUP"),
  NFSPROC_ACCESS(4, "NFSPROC_ACCESS"),
  NFSPROC_READLINK(5, "NFSPROC_READLINK"),
  NFSPROC_READ(6, "NFSPROC_READ"),
  NFSPROC_WRITE(7, "NFSPROC_WRITE"),
  NFSPROC_CREATE(8, "NFSPROC_CREATE"),
  NFSPROC_MKDIR(9, "NFSPROC_MKDIR"),
  NFSPROC_SYMLINK(10, "NFSPROC_SYMLINK"),
  NFSPROC_MKNOD(11, "NFSPROC_MKNOD"),
  NFSPROC_REMOVE(12, "NFSPROC_REMOVE"),
  NFSPROC_RMDIR(13, "NFSPROC_RMDIR"),
  NFSPROC_RENAME(14, "NFSPROC_RENAME"),
  NFSPROC_LINK(15, "NFSPROC_LINK"),
  NFSPROC_READDIR(16, "NFSPROC_READDIR"),
  NFSPROC_READDIRPLUS(17, "NFSPROC_READDIRPLUS"),
  NFSPROC_FSSTAT(18, "NFSPROC_FSSTAT"),
  NFSPROC_FSINFO(19, "NFSPROC_FSINFO"),
  NFSPROC_PATHCONF(20, "NFSPROC_PATHCONF"),
  NFSPROC_COMMIT(21, "NFSPROC_COMMIT");


  private int code;
  private String desc;

  Nfs3Procedure(int code, String desc) {
    this.code = code;
    this.desc = desc;
  }

  public int getCode() {
    return code;
  }

  public String getDesc() {
    return desc;
  }

  private static final Map<Integer, Nfs3Procedure> map = new HashMap<>();

  static {
    for (Nfs3Procedure nfs3Procedure : Nfs3Procedure.values()) {
      map.put(nfs3Procedure.code, nfs3Procedure);
    }
  }

  public static Nfs3Procedure fromCode(int code) {
    Nfs3Procedure nfs3Procedure = map.get(code);
    if (nfs3Procedure == null) {
      throw new IllegalArgumentException("Unknown code: " + code);
    }
    return nfs3Procedure;
  }

  @Override
  public String toString() {
    return name() + "(" + code + ")" + desc;
  }
}
