package com.example.netclient.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString(of = "desc")
public enum NfsStat3 implements BaseEnum {
  NFS3_OK(0, "NFS3 OK"),
  NFS3ERR_PERM(1, "NFS3 Permission"),
  NFS3ERR_NOENT(2, "NFS3 Not Found"),
  NFS3ERR_IO(3, "NFS3 IO"),
  NFS3ERR_NXIO(6, "NFS3 NXIO"),
  NFS3ERR_ACCES(13, "NFS3 Access"),
  NFS3ERR_EXIST(17, "NFS3 Exist"),
  NFS3ERR_XDEV(18, "NFS3 XDev"),
  NFS3ERR_NODEV(19, "NFS3 NodeV"),
  NFS3ERR_NOTDIR(20, "NFS3 NotDir"),
  NFS3ERR_ISDIR(21, "NFS3 IsDir"),
  NFS3ERR_INVAL(22, "NFS3 Invalid"),
  NFS3ERR_FBIG(27, "NFS3 FBIG"),
  NFS3ERR_NOSPC(28, "NFS3 NoSpace"),
  NFS3ERR_ROFS(30, "NFS3 Roof"),
  NFS3ERR_MLINK(31, "NFS3 Link"),
  NFS3ERR_NAMETOOLONG(63, "NFS3 NameToolong"),
  NFS3ERR_NOTEMPTY(66, "NFS3 Not Empty"),
  NFS3ERR_DQUOT(69, "NFS3 DQuot"),
  NFS3ERR_STALE(70, "NFS3 Stale"),
  NFS3ERR_REMOTE(71, "NFS3 Remote"),
  NFS3ERR_BADHANDLE(10001, "NFS3 BadHandle"),
  NFS3ERR_NOT_SYNC(10002, "NFS3 Not Sync"),
  NFS3ERR_BAD_COOKIE(10003, "NFS3 BadCookie"),
  NFS3ERR_NOTSUPP(10004, "NFS3 NotSupp"),
  NFS3ERR_TOOSMALL(10005, "NFS3 TooSmall"),
  NFS3ERR_SERVERFAULT(10006, "NFS3 ServerFault"),
  NFS3ERR_BADTYPE(10007, "NFS3 BadType"),
  NFS3ERR_JUKEBOX(10008, "NFS3 Jukebox");

  private int code;
  private String desc;

}
