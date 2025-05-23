package com.example.netclient;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class FullRpcMessage {
  byte[] xdrData; // The complete XDR encoded RPC message

  public FullRpcMessage(byte[] xdrData) {
    this.xdrData = xdrData;
  }

  // In a real application, you'd have methods here to decode the XDR data
  public void decodeAndPrint() {
    System.out.println("Received complete XDR message (" + xdrData.length + " bytes):");
    // Basic XID and Message Type (assuming no prior errors)
    if (xdrData.length >= 8) {
      ByteBuffer bb = ByteBuffer.wrap(xdrData);
      bb.order(ByteOrder.BIG_ENDIAN); // XDR is Big Endian
      long xid = bb.getInt() & 0xFFFFFFFFL; // Read as unsigned int
      int msgType = bb.getInt();
      int rpcVersion = bb.getInt();
      int programNumber = bb.getInt();
      int programVersion = bb.getInt();
      int procedureNumber = bb.getInt();

      System.out.printf("  XID: 0x%08x (%d)\n", xid, xid);
      System.out.printf("  Message Type: %d (%s)\n", msgType, msgType == 0 ? "CALL" : "REPLY");
      // Further XDR decoding would go here...
    } else {
      System.out.println("  Message too short to decode XID/MsgType.");
    }
    // For now, just print hex
    for (int i = 0; i < Math.min(xdrData.length, 64); i++) { // Print first 64 bytes
      System.out.printf("%02x ", xdrData[i]);
      if ((i + 1) % 16 == 0) System.out.println();
    }
    System.out.println("\n-----------------------------------");
  }
}
