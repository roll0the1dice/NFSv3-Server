package com.example.netclient.model;

import com.example.netclient.enums.NfsStat3;

public class PATHCONF3res extends AbstractNfsResponse<PATHCONF3resok, PATHCONF3resfail>{
  /**
   * Constructor ensures that only one of resok or resfail is set,
   * based on the status.
   *
   * @param status
   * @param resok
   * @param resfail
   */
  private PATHCONF3res(NfsStat3 status, PATHCONF3resok resok, PATHCONF3resfail resfail) {
    super(status, resok, resfail);
  }

  public static PATHCONF3res createOk(PATHCONF3resok okData) {
    return new PATHCONF3res(NfsStat3.NFS3_OK, okData, null);
  }

  public static PATHCONF3res createFail(NfsStat3 failStatus, PATHCONF3resfail failData) {
    if (failStatus == NfsStat3.NFS3_OK) {
      throw new IllegalArgumentException("For failure, status cannot be NFS3_OK");
    }
    return new PATHCONF3res(failStatus, null, failData);
  }
}
