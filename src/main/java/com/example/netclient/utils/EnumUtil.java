package com.example.netclient.utils;

import com.example.netclient.enums.BaseEnum;

import java.util.HashMap;
import java.util.Map;

public class EnumUtil {
  // 使用 ConcurrentHashMap 保证线程安全，并缓存不同枚举类型的查找Map
  private static Map<Class<? extends BaseEnum>, Map<Integer, BaseEnum>> map = new HashMap<>();

  public static <T extends Enum<T> & BaseEnum> T fromCode(Class<T> enumClass, int code) {
    Map<Integer, BaseEnum> enumMap = map.computeIfAbsent(enumClass, k -> {
      Map<Integer, BaseEnum> tMap = new HashMap<>();
      for (T enumConstant : enumClass.getEnumConstants()) {
        tMap.put(enumConstant.getCode(), enumConstant);
      }

      return tMap;
    });

    return (T) enumMap.get(code);
  }
}
