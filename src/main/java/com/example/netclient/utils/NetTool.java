package com.example.netclient.utils;

import io.vertx.core.buffer.Buffer;

public class NetTool {
  public static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02X ", b));
    }
    return sb.toString().trim();
  }

  public static void printHexDump(Buffer buffer) {
    for (int i = 0; i < buffer.length(); i++) {
      System.out.printf("%02X ", buffer.getByte(i));
      if (i % 16 == 0 && i != 0) {
        System.out.println();
      }
    }
    System.out.println();
  }

}
