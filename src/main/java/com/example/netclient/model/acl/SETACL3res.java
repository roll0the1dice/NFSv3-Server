package com.example.netclient.model.acl;

import com.example.netclient.enums.NfsStat3;
import com.example.netclient.model.AbstractNfsResponse;

public class SETACL3res extends AbstractNfsResponse<SETACL3resok, SETACL3resfail> {
  /**
   * Constructor ensures that only one of resok or resfail is set,
   * based on the status.
   *
   * @param status
   * @param resok
   * @param resfail
   */
  public SETACL3res(NfsStat3 status, SETACL3resok resok, SETACL3resfail resfail) {
    super(status, resok, resfail);
  }

  // You might have a more generic constructor and factory methods for clarity
  public static SETACL3res createSuccess(SETACL3resok resok) {
    return new SETACL3res(NfsStat3.NFS3_OK, resok, null);
  }

  public static SETACL3res createFailure(NfsStat3 errorStatus, SETACL3resfail resfail) {
    if (errorStatus == NfsStat3.NFS3_OK) {
      throw new IllegalArgumentException("Error status constructor cannot be used with NFS3_OK");
    }
    return new SETACL3res(errorStatus, null, resfail);
  }

}
