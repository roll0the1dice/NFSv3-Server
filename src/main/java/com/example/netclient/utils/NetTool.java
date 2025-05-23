package com.example.netclient.utils;

import io.vertx.core.buffer.Buffer;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

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

  public static byte[] hexStringToByteArray(String s) throws DecoderException {
    if (s == null) {
      throw new NullPointerException("Input hex string cannot be null");
    }
    // Apache Commons Codec 的 Hex 类通常能处理有无 "0x" 前缀和空格的情况，
    // 但严格来说，它期望的是纯粹的十六进制字符。
    // 为了更稳健，可以先清理字符串。
    if (s.startsWith("0x") || s.startsWith("0X")) {
      s = s.substring(2);
    }
    s = s.replaceAll("\\s+", "");

    if (s.isEmpty()) {
      return new byte[0];
    }
    // Hex.decodeHex 会处理奇数长度的问题，通常会抛出 DecoderException
    return Hex.decodeHex(s);
  }


}
