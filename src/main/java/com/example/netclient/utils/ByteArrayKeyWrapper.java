package com.example.netclient.utils;

import java.util.Arrays;
import java.util.Objects;

public class ByteArrayKeyWrapper {
  private final byte[] data;
  private final int hash;

  public ByteArrayKeyWrapper(byte[] data) {
    Objects.requireNonNull(data, "data must not be null");
    // 创建一个防御副本，以确保键的不可变性
    // 如果传入的原始数组在外部被修改，不会影响 Map 中的键
    this.data = data;
    this.hash = Arrays.hashCode(data);
  }

  public byte[] getData() {
    // 返回一个副本，以保持内部数组的不可变性
    return Arrays.copyOf(data, data.length);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ByteArrayKeyWrapper that = (ByteArrayKeyWrapper) o;
    return Arrays.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public String toString() {
    return "ByteArrayKeyWrapper[" + NetTool.bytesToHex(data) + "]";
  }

}
