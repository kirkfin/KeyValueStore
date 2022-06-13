package kfinley.keyvaluestore;

/***
 * Represents an immutable, durable change log
 */
public interface CommitLog {
    void write(String table, MemTableKey key, Modification modification);
}
