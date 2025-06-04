package com.example.netclient.model;

import io.vertx.core.buffer.Buffer;
import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class RpcFragment {
  private final Buffer data;
  private final boolean isLastFragment;
}
