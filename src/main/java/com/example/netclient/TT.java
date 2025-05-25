package com.example.netclient;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class TT {
    public static void main(String[] args) {
        String hexTimestamp = "0x6832BB5B";
        String parsableHex = hexTimestamp;

        // 1. 去除 "0x" 前缀
        if (hexTimestamp.startsWith("0x") || hexTimestamp.startsWith("0X")) {
            parsableHex = hexTimestamp.substring(2);
        }

        try {
            // 2. 将十六进制转换为十进制秒数
            long secondsSinceEpoch = Long.parseLong(parsableHex, 16);
            System.out.println("原始十六进制: " + hexTimestamp);
            System.out.println("解析用的十六进制: " + parsableHex);
            System.out.println("转换后的秒数 (自1970-01-01 UTC): " + secondsSinceEpoch);

            // 3. 将秒数转换为毫秒数
            long millisSinceEpoch = secondsSinceEpoch * 1000L; // 注意 L 后缀，确保是 long 类型乘法
            System.out.println("转换后的毫秒数: " + millisSinceEpoch);

            // --- 使用 java.time API (推荐) ---
            System.out.println("\n--- 使用 java.time API ---");

            // 创建 Instant 对象 (UTC 时间)
            Instant instant = Instant.ofEpochMilli(millisSinceEpoch);
            System.out.println("UTC 时间 (Instant): " + instant);

            // 转换为系统默认时区的 ZonedDateTime
            ZonedDateTime zonedDateTimeSystemDefault = instant.atZone(ZoneId.systemDefault());
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z ZZZZ");
            System.out.println("本地时区时间: " + zonedDateTimeSystemDefault.format(formatter));

            // 转换为指定时区 (例如上海时间)
            ZonedDateTime zonedDateTimeShanghai = instant.atZone(ZoneId.of("Asia/Shanghai"));
            System.out.println("上海时间: " + zonedDateTimeShanghai.format(formatter));


            // --- 使用旧的 java.util.Date API ---
            System.out.println("\n--- 使用 java.util.Date API ---");
            Date date = new Date(millisSinceEpoch);
            System.out.println("Date 对象: " + date); // 打印的是本地时区

            // 格式化输出 (默认使用本地时区)
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z Z");
            System.out.println("格式化后的本地时间: " + sdf.format(date));

            // 如果需要 UTC 时间的格式化输出
            SimpleDateFormat sdfUtc = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'");
            sdfUtc.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
            System.out.println("格式化后的UTC时间: " + sdfUtc.format(date));


        } catch (NumberFormatException e) {
            System.err.println("无法将十六进制字符串 '" + hexTimestamp + "' 转换为 long: " + e.getMessage());
        }
    }
    
}
