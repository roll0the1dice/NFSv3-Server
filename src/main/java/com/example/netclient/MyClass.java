package com.example.netclient;

import com.example.netclient.model.FSINFO3resok;

import java.util.HashMap;
import java.util.Map;

public class MyClass {
  // 静态变量
  static int staticVar;
  static String staticString;
  static final Map<Integer, String> STATIC_MAP = new HashMap<>();

  // 静态初始化块
  static {
    System.out.println("Static initializer block called.");
    staticVar = 10;
    staticString = "Hello from static block";

    // 复杂的初始化逻辑
    try {
      // 假设这里有一些可能抛出异常的初始化
      STATIC_MAP.put(1, "One");
      STATIC_MAP.put(2, "Two");
      // ...
    } catch (Exception e) {
      System.err.println("Error during static initialization: " + e.getMessage());
    }
  }

  // 另一个静态初始化块 (按顺序执行)
  static {
    System.out.println("Another static initializer block called.");
    // 可以在这里继续初始化或执行其他一次性任务
  }


  public MyClass() {
    System.out.println("Constructor called.");
  }

  public static void staticMethod() {
    System.out.println("Static method called.");
  }

  public static void main(String[] args) {
//    System.out.println("Main method started.");
//    MyClass.staticMethod(); // 触发类加载 (如果尚未加载)
//    System.out.println("Static var: " + MyClass.staticVar);
//    System.out.println("---");
//    MyClass obj1 = new MyClass(); // 触发类加载 (如果尚未加载)
//    MyClass obj2 = new MyClass();
//    System.out.println("Main method finished.");
    FSINFO3resok fsinfo3resok = FSINFO3resok.builder().build();

    System.out.println(fsinfo3resok);

  }
}
