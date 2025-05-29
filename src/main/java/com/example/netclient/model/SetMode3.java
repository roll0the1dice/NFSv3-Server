package com.example.netclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@Data
@AllArgsConstructor
@Builder
public class SetMode3 {
  public int setIt;
  public int mode;
}
