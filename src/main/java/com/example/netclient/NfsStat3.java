package com.example.netclient;

public enum NfsStat3 {
  NFS3_OK(0),
  NFS3ERR_PERM(1),
  NFS3ERR_NOENT(2),
  NFS3ERR_IO(5),
  NFS3ERR_NXIO(6),
  NFS3ERR_ACCES(13); // 注意这个拼写，我在后面的 switch 中会保持一致

  private final int code;

  // 构造函数必须是 private 或包私有
  NfsStat3(int code) {
    this.code = code;
  }

  public int getCode() {
    return this.code;
  }

  // 可选: 从代码获取枚举的静态方法
  public static NfsStat3 fromCode(int code) {
    for (NfsStat3 status : values()) {
      if (status.getCode() == code) {
        return status;
      }
    }
    // 根据你的错误处理策略，可以抛出异常或返回 null/默认值
    throw new IllegalArgumentException("Unknown NfsStat3 code: " + code);
    // return null; // 或者返回 null
  }
}
