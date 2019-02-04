package ru.mail.polis.impl;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Key implements Serializable {
    private final ByteBuffer id;
    private final long bytes;

    @Override
    public boolean equals(Object obj) {
        if (getClass() != obj.getClass())
            return false;
        Key key = (Key) obj;
        return Arrays.equals(id.array(), key.id.array()) && bytes == key.bytes;
    }

    Key(byte[] id, long bytes) {
        this.id = ByteBuffer.wrap(id);
        this.bytes = bytes;
    }

    public ByteBuffer getId() {
        return id;
    }

    Long getBytes() {
        return bytes;
    }

    @Override
    public int hashCode() {
        return (int) (id.hashCode() + bytes);
    }
}

class KeyCustomSerializer implements Serializer<Key>, Serializable {

    @Override
    public Key deserialize(@NotNull DataInput2 input, int available) throws IOException {
        int length = input.readInt();
        byte[] id = new byte[length];
        input.readFully(id);

        long bytes = input.readLong();

        return new Key(id, bytes);
    }

    @Override
    public void serialize(@NotNull DataOutput2 out, @NotNull Key key) throws IOException {
        out.writeInt(key.getId().array().length);
        out.write(key.getId().array());
        out.writeLong(key.getBytes());
    }

}
