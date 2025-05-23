package com.example.netclient;

import lombok.Data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

@Data
public class CredentialsInRPC {
  private int Stamp; // start of body
  private int machineNameLength;
  private String machineName;
  private int UID;
  private int GID;
  private int auxiliaryGIDLength;
  private int auxiliaryGID0;

  private byte[] credentialsData;

  public CredentialsInRPC(byte[] credentialsData) {

    if (credentialsData == null) {
      throw new IllegalArgumentException("credentialsData is null");
    }

    // 存一份副本
    this.credentialsData = Arrays.copyOf(credentialsData, credentialsData.length);

    // 使用 ByteBuffer 进行解析
    ByteBuffer buffer = ByteBuffer.wrap(this.credentialsData);
    buffer.order(ByteOrder.BIG_ENDIAN);


    try {
      this.Stamp = buffer.getInt();
      this.machineNameLength = buffer.getInt();

      if (this.machineNameLength < 0) {
        throw new IllegalArgumentException("Invalid machineNameLength: " + this.machineNameLength +
          ", remaining buffer: " + buffer.remaining());
      }

      int readMultiple4Bytes = ((this.machineNameLength - 1) / 4 + 1) * 4;
      byte[] machineNameBytes = new byte[readMultiple4Bytes];
      buffer.get(machineNameBytes);

      this.machineName = new String(machineNameBytes, "UTF-8").trim();

      this.UID = buffer.getInt();
      this.GID = buffer.getInt();
      this.auxiliaryGIDLength = buffer.getInt();
      this.auxiliaryGID0 = buffer.getInt();

      if (buffer.hasRemaining()) {
        System.err.println("Warning: " + buffer.remaining() + " unparsed bytes remaining in credentialsData.");
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid machineNameLength: " + this.machineNameLength, e);
    }

  }

}
