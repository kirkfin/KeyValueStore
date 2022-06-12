package kfinley.keyvaluestore;

public interface MemTableKeyFactory {
    MemTableKey getKey(String key);
}
