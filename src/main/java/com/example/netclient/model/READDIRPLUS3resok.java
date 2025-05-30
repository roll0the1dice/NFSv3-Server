package com.example.netclient.model;

import com.example.netclient.enums.Nfs3Constant;
import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;
import java.util.List;

@Data
@AllArgsConstructor
@Builder
public class READDIRPLUS3resok implements SerializablePayload {
  private PostOpAttr dirAttributes;
  private long cookieverf;
  private int entriesPresentFlag;
  private List<Entryplus3> entries;
  private int eof;

  @Override
  public void serialize(ByteBuffer buffer) {
    dirAttributes.serialize(buffer);
    buffer.putLong(cookieverf);
    buffer.putInt(entriesPresentFlag);
    for (Entryplus3 entry : entries) {
      entry.serialize(buffer);
    }
    buffer.putInt(eof);
  }

  @Override
  public int getSerializedSize() {
    int totalEntriesSize = entries != null && entries.isEmpty() ? 4 : entries.stream().map(entryplus3 -> entryplus3.getSerializedSize()).reduce(0, Integer::sum);

    return dirAttributes.getSerializedSize() + // dir_attributes
      8 + // cookieverf
      4 + // entries present flag
      totalEntriesSize + // directory entries
      4; // eof flag
  }

  @Override
  public void serialize(Buffer buffer) {
    dirAttributes.serialize(buffer);
    buffer.appendLong(cookieverf);
    buffer.appendInt(entriesPresentFlag);
    for (Entryplus3 entry : entries) {
      entry.serialize(buffer);
    }
    buffer.appendInt(eof);
  }

}
