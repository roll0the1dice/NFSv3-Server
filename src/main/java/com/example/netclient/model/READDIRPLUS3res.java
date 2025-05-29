package com.example.netclient.model;

import com.example.netclient.enums.NfsStat3;

import java.nio.ByteBuffer;
import java.util.Objects;

public class READDIRPLUS3res extends AbstractNfsResponse<READDIRPLUS3resok, READDIRPLUS3resfail> {
  /**
   * Constructor ensures that only one of resok or resfail is set,
   * based on the status.
   *
   * @param status
   * @param resok
   * @param resfail
   */
  private READDIRPLUS3res(NfsStat3 status, READDIRPLUS3resok resok, READDIRPLUS3resfail resfail) {
    super(status, resok, resfail);
  }

  public static READDIRPLUS3res createOk(READDIRPLUS3resok okData) {
    return new READDIRPLUS3res(NfsStat3.NFS3_OK, okData, null);
  }

  public static READDIRPLUS3res createFail(NfsStat3 failStatus, READDIRPLUS3resfail failData) {
    if (failStatus == NfsStat3.NFS3_OK) {
      throw new IllegalArgumentException("For failure, status cannot be NFS3_OK");
    }
    return new READDIRPLUS3res(failStatus, null, failData);
  }
}
