package com.example.netclient.enums;

import java.util.HashMap;
import java.util.Map;

public enum RpcParseState {
  READING_MARKER(0, "Reading Marker"),
  READING_FRAGMENT_DATA(1, "Reading Fragment Data");


  private int code;
  private String desc;

  RpcParseState(int code, String desc) {
    this.code = code;
    this.desc = desc;
  }

  public int getCode() {
    return code;
  }

  public String getDesc() {
    return desc;
  }

  private static final Map<Integer, RpcParseState> map = new HashMap<>();

  static {
    for (RpcParseState rpcParseState : RpcParseState.values()) {
      map.put(rpcParseState.code, rpcParseState);
    }
  }

  public static RpcParseState fromCode(int code) {
    RpcParseState rpcParseState = map.get(code);
    if (rpcParseState == null) {
      throw new IllegalArgumentException("Unknown code: " + code);
    }
    return rpcParseState;
  }

  @Override
  public String toString() {
    return name() + "(" + code + ")" + desc;
  }
}
