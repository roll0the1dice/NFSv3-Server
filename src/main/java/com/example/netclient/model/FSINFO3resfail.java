package com.example.netclient.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class FSINFO3resfail {
  int objAttributes; // post_op_attr present flag
}
