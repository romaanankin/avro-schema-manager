package com.betconstruct;

public class Type {
    private String dataType;
    private String numericPrecision;
    private String numericScele;

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

    public String getNumericScele() {
        return numericScele;
    }

    public void setNumericScele(String numericScele) {
        this.numericScele = numericScele;
    }

    @Override
    public String toString() {
        return "Type{" +
                "dataType='" + dataType + '\'' +
                ", numericPrecision='" + numericPrecision + '\'' +
                ", numericScele='" + numericScele + '\'' +
                '}';
    }
}
