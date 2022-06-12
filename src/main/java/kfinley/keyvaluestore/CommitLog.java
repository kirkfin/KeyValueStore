package kfinley.keyvaluestore;

public interface CommitLog {
    void write(String table, MemTableKey key, Modification modification);
}
