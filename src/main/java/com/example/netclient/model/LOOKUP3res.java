package com.example.netclient.model;

import com.example.netclient.enums.NfsStat3;

import java.nio.ByteBuffer;
import java.util.Objects;

public class LOOKUP3res extends AbstractNfsResponse<LOOKUP3resok, LOOKUP3resfail> {
  /**
   * Constructor ensures that only one of resok or resfail is set,
   * based on the status.
   *
   * @param status
   * @param resok
   * @param resfail
   */
  public LOOKUP3res(NfsStat3 status, LOOKUP3resok resok, LOOKUP3resfail resfail) {
    super(status, resok, resfail);
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

}
