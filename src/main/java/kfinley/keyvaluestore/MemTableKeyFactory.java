package kfinley.keyvaluestore;

/***
 * Decouple Key creation from MemTable implementations
 */
public interface MemTableKeyFactory {
    MemTableKey getKey(String key);
}
