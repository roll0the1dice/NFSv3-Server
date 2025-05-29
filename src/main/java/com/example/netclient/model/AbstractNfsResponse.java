package com.example.netclient.model;

import com.example.netclient.enums.NfsStat3;
import java.io.IOException;
import java.nio.ByteBuffer;

public class AbstractNfsResponse <OK_TYPE extends SerializablePayload, FAIL_TYPE extends  SerializablePayload>{
  private NfsStat3 status;
  private OK_TYPE resok;
  private FAIL_TYPE resfail;

  /**
   * Constructor ensures that only one of resok or resfail is set,
   * based on the status.
   */
  public AbstractNfsResponse(NfsStat3 status, OK_TYPE resok, FAIL_TYPE resfail) {
    this.status = status;
    if (status == NfsStat3.NFS3_OK) {
      if (resok == null) {
        throw new IllegalArgumentException("resok must not be null when status is NFS3_OK");
      }
      this.resok = resok;
      this.resfail = null; // Explicitly nullify the other
    } else {
      if (resfail == null) {
        throw new IllegalArgumentException("resfail must not be null when status is not NFS3_OK");
      }
      this.resfail = resfail;
      this.resok = null; // Explicitly nullify the other
    }
  }

  public final void serialize(ByteBuffer buffer) throws IOException {
    buffer.putInt(status.getCode());

    switch (status) {
      case NFS3_OK:
        if (resok == null) {
          throw new IllegalArgumentException("resok must be not null when status is NFS3_OK");
        }
        resok.serialize(buffer);
        break;
      default:
        if (resfail == null) {
          throw new IllegalArgumentException("resfail must be not null when status is not NFS3_OK");
        }
        resfail.serialize(buffer);
    }
  }

  public final int getSerializedSize() {
    int size = 4; // For status
    if (status == NfsStat3.NFS3_OK) {
      // resok is guaranteed to be non-null here by the constructor
      size += resok.getSerializedSize();
    } else {
      // resfail is guaranteed to be non-null here by the constructor
      size += resfail.getSerializedSize();
    }
    return size;
  }

  // Getter for status if needed
  public NfsStat3 getStatus() {
    return status;
  }

  // Getters for resok/resfail might be useful, but be mindful they can be null
  public OK_TYPE getResok() {
    return resok;
  }

  public FAIL_TYPE getResfail() {
    return resfail;
  }

}
