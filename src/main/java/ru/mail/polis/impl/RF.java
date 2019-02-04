package ru.mail.polis.impl;

import java.util.StringTokenizer;

public class RF {
    private final String errorMsg = "The replicas parameter must contain 2 non-negative integer separated by a symbol \"/\" -> \"ack/from\"\n" +
            "The ack value must be less than or equal to the value of from";
    private final int ack, from;

    RF(int ack, int from) throws IllegalArgumentException {
        this.ack = ack;
        this.from = from;
        validate();
    }

    RF(String replicas) throws IllegalArgumentException {
        StringTokenizer st = new StringTokenizer(replicas, "/");
        if (st.countTokens() != 2) {
            throw new IllegalArgumentException(errorMsg);
        }
        try {
            ack = Integer.parseInt(st.nextToken());
            from = Integer.parseInt(st.nextToken());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(errorMsg);
        }
        validate();
    }

    private void validate() throws IllegalArgumentException {
        if (ack > from || ack <= 0) {
            throw new IllegalArgumentException(errorMsg);
        }
    }

    int getFrom() {
        return from;
    }

    public int getAck() {
        return ack;
    }

    @Override
    public String toString() {
        return "RF{" +
                "ack=" + ack +
                ", from=" + from +
                '}';
    }
}