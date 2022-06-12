package kfinley.keyvaluestore;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

public class MemTableKey128BitHash implements MemTableKey {

    private final String key;
    private final HashCode hashCode;

    MemTableKey128BitHash(String key) {
        this.key = key;
        this.hashCode = Hashing.murmur3_128().hashString(key, StandardCharsets.UTF_8);
    }

    @Override
    public long getHash() {
        return hashCode.asLong();
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public int compareTo(MemTableKey o) {
        return Long.compare(getHash(), o.getHash());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemTableKey128BitHash key1 = (MemTableKey128BitHash) o;
        return key.equals(key1.key);
    }

    @Override
    public int hashCode() {
        return hashCode.asInt();
    }
}
