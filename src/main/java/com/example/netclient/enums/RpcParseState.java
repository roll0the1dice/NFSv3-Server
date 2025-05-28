package com.example.netclient.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString(of = "desc")
public enum RpcParseState implements BaseEnum {
  READING_MARKER(0, "Reading Marker"),
  READING_FRAGMENT_DATA(1, "Reading Fragment Data");


  private int code;
  private String desc;
}
