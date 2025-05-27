package com.example.netclient.enums;

import java.util.HashMap;
import java.util.Map;

public enum RpcReplyMessage {
  MSG_TYPE_REPLY(1, "reply OK"),
  REPLY_STAT_MSG_ACCEPTED(0, "Reply Stat Message Accepted"),
  VERF_FLAVOR_AUTH_NONE(0, "Verf Flavor Auth None"),
  VERF_LENGTH_ZERO(0, "Verf Length Zero"),
  ACCEPT_STAT_SUCCESS(0, "Accept Stat Success");

  private final int code;
  private String desc;

  RpcReplyMessage(int code, String desc) {
    this.code = code;
    this.desc = desc;
  }

  private static Map<Integer, RpcReplyMessage> map = new HashMap<Integer, RpcReplyMessage>();

  static {
    for (RpcReplyMessage constant : RpcReplyMessage.values()) {
      map.put(constant.code, constant);
    }
  }

  public static RpcReplyMessage fromCode(int code) {
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
