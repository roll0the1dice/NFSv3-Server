package com.example.netclient.model;

import com.example.netclient.enums.NfsStat3;

import java.nio.ByteBuffer;
import java.util.Objects;

public class READ3res extends AbstractNfsResponse<READ3resok, READ3resfail> {
  /**
   * Constructor ensures that only one of resok or resfail is set,
   * based on the status.
   *
   * @param status
   * @param resok
   * @param resfail
   */
  public READ3res(NfsStat3 status, READ3resok resok, READ3resfail resfail) {
    super(status, resok, resfail);
  }

  public static READ3res createOk(READ3resok okData) {
    return new READ3res(NfsStat3.NFS3_OK, okData, null);
  }

  public static READ3res createFail(NfsStat3 failStatus, READ3resfail failData) {
    if (failStatus == NfsStat3.NFS3_OK) {
      throw new IllegalArgumentException("For failure, status cannot be NFS3_OK");
    }
    return new READ3res(failStatus, null, failData);
  }

}
