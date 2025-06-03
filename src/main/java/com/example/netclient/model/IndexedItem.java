package com.example.netclient.model;

import lombok.Data;

@Data
public class IndexedItem<T> {
  final int index;
  final T value;

 public IndexedItem(int index, T value) {
   this.index = index;
   this.value = value;
 }

  @Override
  public String toString() {
    return "IndexedItem{index=" + index + ", value=" + value + '}';
  }
}
