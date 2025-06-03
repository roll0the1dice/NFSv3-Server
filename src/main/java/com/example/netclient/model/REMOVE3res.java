package com.example.netclient.model;

import com.example.netclient.enums.NfsStat3;

public class REMOVE3res extends AbstractNfsResponse<REMOVE3resok, REMOVE3resfail>{
  /**
   * Constructor ensures that only one of resok or resfail is set,
   * based on the status.
   *
   * @param status
   * @param resok
   * @param resfail
   */
  public REMOVE3res(NfsStat3 status, REMOVE3resok resok, REMOVE3resfail resfail) {
    super(status, resok, resfail);
  }
  public static REMOVE3res createOk(REMOVE3resok okData) {
    return new REMOVE3res(NfsStat3.NFS3_OK, okData, null);
  }

  public static REMOVE3res createFail(NfsStat3 failStatus, REMOVE3resfail failData) {
    if (failStatus == NfsStat3.NFS3_OK) {
      throw new IllegalArgumentException("For failure, status cannot be NFS3_OK");
    }
    return new REMOVE3res(failStatus, null, failData);
  }
}
