package kfinley.keyvaluestore;

public class Modification implements Comparable<Modification> {

    private final String modification;
    private final long timestamp;

    public Modification(String modification) {
        this.modification = modification;
        timestamp = System.nanoTime();
    }

    public String getModification() {
        return modification;
    }

    public long getSizeBytes() {
        if (modification == null) {
            return 0;
        }
        return modification.length();
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(Modification o) {
        return Long.compare(getTimestamp(), o.getTimestamp());
    }
}
