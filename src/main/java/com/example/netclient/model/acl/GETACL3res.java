package com.example.netclient.model.acl;

import com.example.netclient.enums.NfsStat3;
import com.example.netclient.model.AbstractNfsResponse;
import com.example.netclient.model.CREATE3res;
import com.example.netclient.model.CREATE3resfail;
import com.example.netclient.model.CREATE3resok;

public class GETACL3res extends AbstractNfsResponse<GETACL3resok, GETACL3resfail> {
  /**
   * Constructor ensures that only one of resok or resfail is set,
   * based on the status.
   *
   * @param status
   * @param resok
   * @param resfail
   */
  private GETACL3res(NfsStat3 status, GETACL3resok resok, GETACL3resfail resfail) {
    super(status, resok, resfail);
  }

  // You might have a more generic constructor and factory methods for clarity
  public static GETACL3res createSuccess(GETACL3resok resok) {
    return new GETACL3res(NfsStat3.NFS3_OK, resok, null);
  }

  public static GETACL3res createFailure(NfsStat3 errorStatus, GETACL3resfail resfail) {
    if (errorStatus == NfsStat3.NFS3_OK) {
      throw new IllegalArgumentException("Error status constructor cannot be used with NFS3_OK");
    }
    return new GETACL3res(errorStatus, null, resfail);
  }

}
