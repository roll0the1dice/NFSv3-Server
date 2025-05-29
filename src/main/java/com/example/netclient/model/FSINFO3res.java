package com.example.netclient.model;

import com.example.netclient.enums.NfsStat3;

import java.nio.ByteBuffer;
import java.util.Objects;

public class FSINFO3res extends AbstractNfsResponse<FSINFO3resok, FSINFO3resfail> {

  /**
   * Constructor ensures that only one of resok or resfail is set,
   * based on the status.
   *
   * @param status
   * @param resok
   * @param resfail
   */
  public FSINFO3res(NfsStat3 status, FSINFO3resok resok, FSINFO3resfail resfail) {
    super(status, resok, resfail);
  }

  public static FSINFO3res createOk(FSINFO3resok okData) {
    return new FSINFO3res(NfsStat3.NFS3_OK, okData, null);
  }

  public static FSINFO3res createFail(NfsStat3 failStatus, FSINFO3resfail failData) {
    if (failStatus == NfsStat3.NFS3_OK) {
      throw new IllegalArgumentException("For failure, status cannot be NFS3_OK");
    }
    return new FSINFO3res(failStatus, null, failData);
  }

}
