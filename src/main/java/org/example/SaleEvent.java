package org.example;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class SaleEvent {
    private String sellerId;
    private Double amountUsd;

    private Double amountTND;
    private long saleTimestamp;

    // Constructors, getters, setters...

    public String getSellerId() {
        return sellerId;
    }

    public void setSellerId(String sellerId) {
        this.sellerId = sellerId;
    }

    public Double getAmountUsd() {
        return amountUsd;
    }

    public void setAmountUsd(Double amountUsd) {
        this.amountUsd = amountUsd;
    }

    public Double getAmountTND() {
        return amountTND;
    }

    public void setAmountTND() {
        this.amountTND = this.amountUsd * 3.0;
    }

    public long getSaleTimestamp() {
        return saleTimestamp;
    }

    public void setSaleTimestamp(long saleTimestamp) {
        this.saleTimestamp = saleTimestamp;
    }

    public void setAmountDinar(Double rate) {
        if (rate == null) {
            this.amountTND = null;
            return;
        }
        this.amountTND = this.amountUsd * rate;

    }

    public String toJson() {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(this);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}