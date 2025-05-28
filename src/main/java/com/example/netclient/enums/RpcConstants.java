package com.example.netclient.enums;

public class RpcConstants {
  public static final int MSG_TYPE_REPLY = 1;          // RPC消息类型：回复
  public static final int REPLY_STAT_MSG_ACCEPTED = 0; // 回复状态：消息被接受
  public static final int VERF_FLAVOR_AUTH_NONE = 0;   // 认证类型：无认证
  public static final int VERF_LENGTH_ZERO = 0;        // 认证数据长度：0
  public static final int ACCEPT_STAT_SUCCESS = 0;     // 接受状态：成功

  public static final int RPC_ACCEPTED_REPLY_HEADER_LENGTH = 24; // 6 * 4 bytes
}
