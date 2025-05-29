package com.example.netclient.model;

import com.example.netclient.enums.NfsStat3;

import java.nio.ByteBuffer;
import java.util.Objects;

public class ACCESS3res extends AbstractNfsResponse<ACCESS3resok, ACCESS3resfail> {

  /**
   * Constructor ensures that only one of resok or resfail is set,
   * based on the status.
   *
   * @param status
   * @param resok
   * @param resfail
   */
  public ACCESS3res(NfsStat3 status, ACCESS3resok resok, ACCESS3resfail resfail) {
    super(status, resok, resfail);
  }

  public static ACCESS3res createOk(ACCESS3resok okData) {
    return new ACCESS3res(NfsStat3.NFS3_OK, okData, null);
  }

  public static ACCESS3res createFail(NfsStat3 failStatus, ACCESS3resfail failData) {
    if (failStatus == NfsStat3.NFS3_OK) {
      throw new IllegalArgumentException("For failure, status cannot be NFS3_OK");
    }
    return new ACCESS3res(failStatus, null, failData);
  }

}
