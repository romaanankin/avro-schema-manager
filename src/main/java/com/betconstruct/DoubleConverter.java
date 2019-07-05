package com.betconstruct;

import java.math.BigDecimal;

public class DoubleConverter {
    static BigDecimal convertToLong(Double aDouble) {
        int i = scaleValue(aDouble);
        return new BigDecimal(aDouble * (Math.pow(10,i)));
    }

    static int scaleValue(Double aDouble) {
        String text = Double.toString(Math.abs(aDouble));
        int integerPlaces = text.indexOf('.');
        return text.length() - integerPlaces - 1;
    }
}
