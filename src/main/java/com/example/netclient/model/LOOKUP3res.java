package com.example.netclient.model;

import com.example.netclient.enums.NfsStat3;

import java.nio.ByteBuffer;
import java.util.Objects;

public class LOOKUP3res {
  private final NfsStat3 status;
  private final LOOKUP3resok resok;     // Nullable
  private final LOOKUP3resfail resfail; // Nullable

  private LOOKUP3res(NfsStat3 status, LOOKUP3resok resok, LOOKUP3resfail resfail) {
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

  public static LOOKUP3res createOk(LOOKUP3resok okData) {
    return new LOOKUP3res(NfsStat3.NFS3_OK, okData, null);
  }

  public static LOOKUP3res createFail(NfsStat3 failStatus, LOOKUP3resfail failData) {
    if (failStatus == NfsStat3.NFS3_OK) {
      throw new IllegalArgumentException("For failure, status cannot be NFS3_OK");
    }
    return new LOOKUP3res(failStatus, null, failData);
  }

  public void serialize(ByteBuffer buffer) {
    buffer.putInt(status.getCode());

    switch (status) {
      case NFS3_OK:
        if (resok == null) {
          throw new IllegalArgumentException("resok must be not null when status is NFS3_OK");
        }
        resok.serialize(buffer);
        break;
      case NFS3ERR_NOENT:
        if (resfail == null) {
          throw new IllegalArgumentException("resfail must be not null when status is NFS3ERR_NOENT");
        }
        resfail.serialize(buffer);
        break;
      default:
        throw new IllegalArgumentException("For failure, status is not illegal");
    }
  }

  public int getSerializedSize() {
    if (status == NfsStat3.NFS3_OK) {
      if (resok == null) {
        throw new IllegalArgumentException("resok must be not null when status is NFS3_OK");
      }
      return 4 + // status
        resok.getSerializedSize();
    }
    if (resfail == null) {
      throw new IllegalArgumentException("resfail must be not null when status is NFS3ERR_NOENT");
    }

    return 4 + resfail.getSerializedSize();
  }
}
