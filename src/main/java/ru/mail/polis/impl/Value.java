package ru.mail.polis.impl;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.io.Serializable;

public class Value implements Serializable {
    private final byte[] data;
    private final long timestamp;
    private final State state;

    public Value(byte[] data, long timestamp, State state) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = state;
    }

    public State getState() { return state; }

    public byte[] getData() {
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public enum State {
        PRESENT,
        REMOVED,
        ABSENT
    }
}

class ValueCustomSerializer implements Serializer<Value>, Serializable {

    @Override
    public Value deserialize(@NotNull DataInput2 input, int available) throws IOException {
        int length = input.readInt();
        byte[] data = new byte[length];
        input.readFully(data);

        long timestamp = input.readLong();

        Value.State state = Value.State.valueOf(input.readUTF());

        return new Value(data, timestamp, state);
    }

    @Override
    public void serialize(@NotNull DataOutput2 out, @NotNull Value value) throws IOException {
        out.writeInt(value.getData().length);
        out.write(value.getData());
        out.writeLong(value.getTimestamp());
        out.writeUTF(value.getState().name());
    }

}