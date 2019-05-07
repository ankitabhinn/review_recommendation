package com.bigdata.finalproject.util;

public class StringUtil {

    public static String convertStringToInteger(String str) {
        StringBuilder stringBuilder = new StringBuilder();
        for (char c : str.toCharArray()) {
            int diff = Integer.MIN_VALUE;
            if (c >= 65 && c <= 90) {
                diff = (int) c - 64;
            }
            if (diff > 0) {
                stringBuilder.append(diff);
            } else {
                stringBuilder.append(c);
            }
        }
        return stringBuilder.toString();
    }

}
