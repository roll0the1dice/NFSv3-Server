package com.example.netclient.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString(of = "desc")
public enum FType3 implements BaseEnum {
  NF3REG(1, "NF3REG"),
  NF3DIR(2, "NF3DIR"),
  NF3BLK(3, "NF3BLK"),
  NF3CHR(4, "NF3CHR"),
  NF3LNK(5, "NF3LNK"),
  NF3SOCK(6, "NF3SOCK"),
  NF3FIFO(7, "NF3FIFO");

  private final int code;
  private final String desc;
}
