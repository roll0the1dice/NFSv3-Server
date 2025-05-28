package com.example.netclient;

import java.lang.reflect.Method;

class MClass {
  public void doSomething(String name, int age, boolean isActive) {
    // ...
  }
  public void noParamsMethod() {
    // ...
  }
}

public class ReflectionExample {
  public static void main(String[] args) {
    try {
      // 1. 获取 MyClass 的 Class 对象
      Class<?> myClassObj = MClass.class;

      // 2. 获取 doSomething 方法的 Method 对象
      // 需要指定参数类型以区分重载方法
      Method method = myClassObj.getMethod("doSomething", String.class, int.class, boolean.class);

      // 3. 获取参数类型
      Class<?>[] paramTypes = method.getParameterTypes(); // 这就是你问的那行代码

      // 4. 打印参数类型
      System.out.println("Parameters for 'doSomething':");
      for (Class<?> paramType : paramTypes) {
        System.out.println(paramType.getName()); // e.g., java.lang.String, int, boolean
      }

      System.out.println("\n---");

      Method noParamsMethod = myClassObj.getMethod("noParamsMethod");
      Class<?>[] noParamTypesArray = noParamsMethod.getParameterTypes();
      System.out.println("Parameters for 'noParamsMethod' (length): " + noParamTypesArray.length); // 输出 0


    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    }
  }
}
