package com.example.netclient.model;

import com.example.netclient.enums.NfsStat3;

public class SETATTR3res extends AbstractNfsResponse<SETATTR3resok, SETATTR3resfail> {
  /**
   * Constructor ensures that only one of resok or resfail is set,
   * based on the status.
   *
   * @param status
   * @param resok
   * @param resfail
   */
  private SETATTR3res(NfsStat3 status, SETATTR3resok resok, SETATTR3resfail resfail) {
    super(status, resok, resfail);
  }

  // You might have a more generic constructor and factory methods for clarity
  public static SETATTR3res createSuccess(SETATTR3resok resok) {
    return new SETATTR3res(NfsStat3.NFS3_OK, resok, null);
  }

  public static SETATTR3res createFailure(NfsStat3 errorStatus, SETATTR3resfail resfail) {
    if (errorStatus == NfsStat3.NFS3_OK) {
      throw new IllegalArgumentException("Error status constructor cannot be used with NFS3_OK");
    }
    return new SETATTR3res(errorStatus, null, resfail);
  }
}
