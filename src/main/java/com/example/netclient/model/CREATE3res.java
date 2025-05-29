package com.example.netclient.model;

import com.example.netclient.enums.NfsStat3;

public class CREATE3res extends AbstractNfsResponse<CREATE3resok, CREATE3resfail>{

  /**
   * Constructor ensures that only one of resok or resfail is set,
   * based on the status.
   *
   * @param status
   * @param resok
   * @param resfail
   */
  private CREATE3res(NfsStat3 status, CREATE3resok resok, CREATE3resfail resfail) {
    super(status, resok, resfail);
  }

  // You might have a more generic constructor and factory methods for clarity
  public static CREATE3res createSuccess(CREATE3resok resok) {
    return new CREATE3res(NfsStat3.NFS3_OK, resok, null);
  }

  public static CREATE3res createFailure(NfsStat3 errorStatus, CREATE3resfail resfail) {
    if (errorStatus == NfsStat3.NFS3_OK) {
      throw new IllegalArgumentException("Error status constructor cannot be used with NFS3_OK");
    }
    return new CREATE3res(errorStatus, null, resfail);
  }
}
