package com.example.netclient.model;

import com.example.netclient.enums.NfsStat3;

public class GETATTR3res extends AbstractNfsResponse<GETATTR3resok, GETATTR3resfail>{
  /**
   * Constructor ensures that only one of resok or resfail is set,
   * based on the status.
   *
   * @param status
   * @param resok
   * @param resfail
   */
  private GETATTR3res(NfsStat3 status, GETATTR3resok resok, GETATTR3resfail resfail) {
    super(status, resok, resfail);
  }

  public static GETATTR3res createOk(GETATTR3resok okData) {
    return new GETATTR3res(NfsStat3.NFS3_OK, okData, null);
  }

  public static GETATTR3res createFail(NfsStat3 failStatus, GETATTR3resfail failData) {
    if (failStatus == NfsStat3.NFS3_OK) {
      throw new IllegalArgumentException("For failure, status cannot be NFS3_OK");
    }
    return new GETATTR3res(failStatus, null, failData);
  }
}
