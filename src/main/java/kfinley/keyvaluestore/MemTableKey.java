package kfinley.keyvaluestore;

/***
 * String key abstraction for hashing and sorting within a MemTable and SSTable
 */
public interface MemTableKey extends Comparable<MemTableKey> {
    long getHash();

    String getKey();
}
