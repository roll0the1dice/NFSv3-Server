package com.example.netclient.model;

import com.example.netclient.enums.NfsStat3;

import java.nio.ByteBuffer;
import java.util.Objects;

public class FSINFO3res  {
  private final NfsStat3 status;
  private final FSINFO3resok resok;     // Nullable
  private final FSINFO3resfail resfail; // Nullable

  private FSINFO3res(NfsStat3 status, FSINFO3resok resok, FSINFO3resfail resfail) {
    this.status = Objects.requireNonNull(status, "Status cannot be null");
    this.resok = resok;
    this.resfail = resfail;

    // 确保逻辑一致性：如果 status 是 OK，则 resok 必须非空，resfail 必须为空，反之亦然。
    if (status == NfsStat3.NFS3_OK) {
      Objects.requireNonNull(resok, "resok cannot be null when status is NFS3_OK");
      if (resfail != null) {
        throw new IllegalArgumentException("resfail must be null when status is NFS3_OK");
      }
    } else {
      Objects.requireNonNull(resfail, "resfail cannot be null when status is not NFS3_OK");
      if (resok != null) {
        throw new IllegalArgumentException("resok must be null when status is not NFS3_OK");
      }
    }
  }

  public static FSINFO3res createOk(FSINFO3resok okData) {
    return new FSINFO3res(NfsStat3.NFS3_OK, okData, null);
  }

  public static FSINFO3res createFail(NfsStat3 failStatus, FSINFO3resfail failData) {
    if (failStatus == NfsStat3.NFS3_OK) {
      throw new IllegalArgumentException("For failure, status cannot be NFS3_OK");
    }
    return new FSINFO3res(failStatus, null, failData);
  }

  public void serialize(ByteBuffer buffer) {
    if (resok == null) {
      throw new IllegalArgumentException("resok must be null when status is not NFS3_OK");
    }

    buffer.putInt(status.getCode());
    resok.serialize(buffer);
  }

  public int getSerializedSize() {
    if (status == NfsStat3.NFS3_OK) {
      return 4 + // status
      FSINFO3resok.getSerializedSize();
    }
    return 4 + // status
      4; // post_op_attr
  }

}
