package com.example.netclient.model;

import com.example.netclient.enums.Nfs3Constant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;
import java.util.List;

@Data
@AllArgsConstructor
@Builder
public class READDIRPLUS3resok {
  int dirAttrPresentFlag;
  FAttr3 dirAttr;
  long cookieverf;
  int entriesPresentFlag;
  List<Entryplus3> entries;
  int eof;

  public void serialize(ByteBuffer buffer) {
    buffer.putInt(dirAttrPresentFlag);
    dirAttr.serialize(buffer);
    buffer.putLong(cookieverf);
    buffer.putInt(entriesPresentFlag);
    for (Entryplus3 entry : entries) {
      entry.serialize(buffer);
    }
    buffer.putInt(eof);
  }

  public int getSerializedSize() {
    int totalEntriesSize = entries != null && entries.isEmpty() ? 4 : entries.stream().map(entryplus3 -> entryplus3.getSerializedSize()).reduce(0, Integer::sum);

    return 4 + // dir_attributes present flag
      Nfs3Constant.DIR_ATTR_SIZE + // dir_attributes
      8 + // cookieverf
      4 + // entries present flag
      totalEntriesSize + // directory entries
      4; // eof flag
  }

}
