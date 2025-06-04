package com.example.netclient.model;

import com.example.netclient.enums.NfsStat3;

public class MKDIR3res extends AbstractNfsResponse<MKDIR3resok, MKDIR3resfail>{
  /**
   * Constructor ensures that only one of resok or resfail is set,
   * based on the status.
   *
   * @param status
   * @param resok
   * @param resfail
   */
  public MKDIR3res(NfsStat3 status, MKDIR3resok resok, MKDIR3resfail resfail) {
    super(status, resok, resfail);
  }

  public static MKDIR3res createOk(MKDIR3resok okData) {
    return new MKDIR3res(NfsStat3.NFS3_OK, okData, null);
  }

  public static MKDIR3res createFail(NfsStat3 failStatus, MKDIR3resfail failData) {
    if (failStatus == NfsStat3.NFS3_OK) {
      throw new IllegalArgumentException("For failure, status cannot be NFS3_OK");
    }
    return new MKDIR3res(failStatus, null, failData);
  }
}
