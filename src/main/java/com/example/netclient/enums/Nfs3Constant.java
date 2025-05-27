package com.example.netclient.enums;

public final class Nfs3Constant {
  public static final int FILE_ATTR_SIZE = 4 + // type
    4 + // mode
    4 + // nlink
    4 + // uid
    4 + // gid
    8 + // size
    8 + // used
    8 + // rdev
    4 + // fsid (major)
    4 + // fsid (minor)
    8 + // fileid
    4 + // atime (seconds)
    4 + // atime (nseconds)
    4 + // mtime (seconds)
    4 + // mtime (nseconds)
    4 + // ctime (seconds)
    4;  // ctime

  public static final int DIR_ATTR_SIZE = 4 + // type
    4 + // mode
    4 + // nlink
    4 + // uid
    4 + // gid
    8 + // size
    8 + // used
    8 + // rdev
    4 + // fsid (major)
    4 + // fsid (minor)
    8 + // fileid
    4 + // atime (seconds)
    4 + // atime (nseconds)
    4 + // mtime (seconds)
    4 + // mtime (nseconds)
    4 + // ctime (seconds)
    4;  // ctime (nseconds)

  public static final int NAME_ATTR_SIZE = 4 + // type
    4 + // mode
    4 + // nlink
    4 + // uid
    4 + // gid
    8 + // size
    8 + // used
    8 + // rdev
    4 + // fsid (major)
    4 + // fsid (minor)
    8 + // fileid
    4 + // atime (seconds)
    4 + // atime (nseconds)
    4 + // mtime (seconds)
    4 + // mtime (nseconds)
    4 + // ctime (seconds)
    4;  // ctime (nseconds)
}
