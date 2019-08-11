package com;

import java.math.BigDecimal;

public class DoubleConverterUtil {
    static BigDecimal convertToLong(Double aDouble) {
        int i = scaleValue(aDouble);
        return new BigDecimal(aDouble * (Math.pow(10,i)));
    }

    private static int scaleValue(Double aDouble) {
        String text = Double.toString(Math.abs(aDouble));
        int integerPlaces = text.indexOf('.');
        return text.length() - integerPlaces - 1;
    }
}
