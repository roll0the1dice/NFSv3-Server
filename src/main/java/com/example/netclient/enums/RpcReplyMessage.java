package com.example.netclient.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString(of = "desc")
public enum RpcReplyMessage implements BaseEnum {
  MSG_TYPE_REPLY(1, "reply OK"),
  REPLY_STAT_MSG_ACCEPTED(0, "Reply Stat Message Accepted"),
  VERF_FLAVOR_AUTH_NONE(0, "Verf Flavor Auth None"),
  VERF_LENGTH_ZERO(0, "Verf Length Zero"),
  ACCEPT_STAT_SUCCESS(0, "Accept Stat Success");

  private final int code;
  private String desc;
}
