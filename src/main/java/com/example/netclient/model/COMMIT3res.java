package com.example.netclient.model;

import com.example.netclient.enums.NfsStat3;

import java.nio.ByteBuffer;
import java.util.Objects;

public class COMMIT3res extends AbstractNfsResponse<COMMIT3resok, COMMIT3resfail> {
  /**
   * Constructor ensures that only one of resok or resfail is set,
   * based on the status.
   *
   * @param status
   * @param resok
   * @param resfail
   */
  private COMMIT3res(NfsStat3 status, COMMIT3resok resok, COMMIT3resfail resfail) {
    super(status, resok, resfail);
  }

  // You might have a more generic constructor and factory methods for clarity
  public static COMMIT3res createSuccess(COMMIT3resok resok) {
    return new COMMIT3res(NfsStat3.NFS3_OK, resok, null);
  }

  public static COMMIT3res createFailure(NfsStat3 errorStatus, COMMIT3resfail resfail) {
    if (errorStatus == NfsStat3.NFS3_OK) {
      throw new IllegalArgumentException("Error status constructor cannot be used with NFS3_OK");
    }
    return new COMMIT3res(errorStatus, null, resfail);
  }

}
