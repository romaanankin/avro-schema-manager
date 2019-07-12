package com.betconstruct;

public class Type {
    private String dataType;
    private String numericPrecision;
    private String numericScale;

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getNumericPrecision() {
        return numericPrecision;
    }

    public void setNumericPrecision(String numericPrecision) {
        this.numericPrecision = numericPrecision;
    }

    public String getNumericScale() {
        return numericScale;
    }

    public void setNumericScale(String numericScale) {
        this.numericScale = numericScale;
    }

    @Override
    public String toString() {
        return "Type{" +
                "dataType='" + dataType + '\'' +
                ", numericPrecision='" + numericPrecision + '\'' +
                ", numericScale='" + numericScale + '\'' +
                '}';
    }
}
