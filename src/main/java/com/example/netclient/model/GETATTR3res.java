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

  public static GETATTR3res createSuccess(GETATTR3resok resok) {
    return new GETATTR3res(NfsStat3.NFS3_OK, resok, null);
  }
}
