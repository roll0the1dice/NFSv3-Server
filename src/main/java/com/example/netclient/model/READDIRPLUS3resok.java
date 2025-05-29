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
  PostOpAttr dirAttributes;
  long cookieverf;
  int entriesPresentFlag;
  List<Entryplus3> entries;
  int eof;

  public void serialize(ByteBuffer buffer) {
    dirAttributes.serialize(buffer);
    buffer.putLong(cookieverf);
    buffer.putInt(entriesPresentFlag);
    for (Entryplus3 entry : entries) {
      entry.serialize(buffer);
    }
    buffer.putInt(eof);
  }

  public int getSerializedSize() {
    int totalEntriesSize = entries != null && entries.isEmpty() ? 4 : entries.stream().map(entryplus3 -> entryplus3.getSerializedSize()).reduce(0, Integer::sum);

    return dirAttributes.getSerializedSize() + // dir_attributes
      8 + // cookieverf
      4 + // entries present flag
      totalEntriesSize + // directory entries
      4; // eof flag
  }

}
