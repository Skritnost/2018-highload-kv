package ru.mail.polis.impl;

import org.jetbrains.annotations.NotNull;
import org.mapdb.*;
import ru.mail.polis.KVDao;
import java.io.File;
import java.util.NoSuchElementException;

//DBMaker

public class KVDaoImpl implements KVDao {

    private final DB db;
    private final HTreeMap<Key, Value> storage;

    public KVDaoImpl(File data) {
        File dataBase = new File(data, "dataBase");
        Serializer<Value> valueSerializer = new ValueCustomSerializer();
        Serializer<Key> keySerializer = new KeyCustomSerializer();
        this.db = DBMaker
                .fileDB(dataBase)
                .fileMmapEnableIfSupported()
                .fileMmapPreclearDisable()
                .fileChannelEnable()
                .closeOnJvmShutdown()
                .make();
        this.storage = db.hashMap(data.getName())
                .keySerializer(keySerializer)
                .valueSerializer(valueSerializer)
                .createOrOpen();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IllegalStateException {
        Value value = internalGet(key, 0);

        if (value.getState() == Value.State.ABSENT || value.getState() == Value.State.REMOVED) {
            throw new NoSuchElementException();
        }
        return value.getData();
    }

    @NotNull
    public Value internalGet(@NotNull byte[] key, long bytes) {
        Value value = storage.get(new Key(key, bytes));
        if (value == null) {
            return new Value(new byte[]{}, 0, Value.State.ABSENT);
        }
        return value;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        upsert(key, 0, value);
    }

    public void upsert(@NotNull byte[] id, long bytes, @NotNull byte[] value) {
        storage.put(new Key(id, bytes), new Value(value, System.currentTimeMillis(), Value.State.PRESENT));
    }

    @Override
    public void remove(@NotNull byte[] key) {
        remove(key, 0);
    }

    public void remove(@NotNull byte[] key, long bytes) {
        storage.put(new Key(key, bytes), new Value(new byte[]{}, System.currentTimeMillis(), Value.State.REMOVED));
    }

    @Override
    public void close() {
        db.close();
    }
}
