package kfinley.keyvaluestore;

public interface MemTableKey extends Comparable<MemTableKey> {
    long getHash();

    String getKey();
}
