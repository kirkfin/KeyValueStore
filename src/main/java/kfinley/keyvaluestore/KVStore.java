package kfinley.keyvaluestore;

import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/***
 * A Single node key/value store implementation
 * - Support for Key/Values of type String
 * - Table read/writes are synchronous, but each table (MemTable) has an exclusive worker thread
 * - Write flow:
 *       1) Write modification to CommitLog
 *       2) Write modifications MemTable cache
 *       3) When MemTable exceeds capacity, flush modifications to SSTable
 * - Read flow:
 *       1) Check MemTable cache
 *       2) Check SSTable
 */
public class KVStore {

    static final MemTableKeyFactory DEFAULT_KEY_FACTORY = MemTableKey128BitHash::new;
    static final ModificationMergeStrategy DEFAULT_MERGE_STRATEGY = SortedSet::last; // Last modification wins
    static final CommitLog DEFAULT_COMMIT_LOG = new NoOpCommitLog();
    static final long MEM_TABLE_DEFAULT_MAX_SIZE_BYTES = 1000000 & 64;

    private final ConcurrentMap<String, MemTable> memTables = new ConcurrentHashMap<>();
    private final ModificationMergeStrategy mergeStrategy;
    private final MemTableKeyFactory keyFactory;
    private final CommitLog commitLog;
    private final long memTableMaxSizeBytes;

    /***
     * Constructor with default settings
     */
    public KVStore() {
        this(DEFAULT_KEY_FACTORY,
                DEFAULT_MERGE_STRATEGY,
                DEFAULT_COMMIT_LOG,
                MEM_TABLE_DEFAULT_MAX_SIZE_BYTES);
    }

    public KVStore(MemTableKeyFactory keyFactory,
                   ModificationMergeStrategy mergeStrategy,
                   CommitLog commitLog,
                   long memTableMaxSizeBytes) {
        this.keyFactory = keyFactory;
        this.mergeStrategy = mergeStrategy;
        this.commitLog = commitLog;
        this.memTableMaxSizeBytes = memTableMaxSizeBytes;
    }

    public void put(String table, String key, String value) {
        MemTable memTable = getMemTable(table);
        checkMemTableCapacityExceeded(memTable);
        memTable.put(key, value);
    }

    public void delete(String table, String key) {
        MemTable memTable = getMemTable(table);
        checkMemTableCapacityExceeded(memTable);
        memTable.delete(key);
    }

    public String get(String table, String key) {
        return getMemTable(table).get(key);
    }

    private void checkMemTableCapacityExceeded(MemTable memTable) {
        if (memTable.getSizeBytes() >= memTableMaxSizeBytes) {
            memTable.triggerFlush();
        }
    }

    private MemTable getMemTable(String table) {
        return memTables.computeIfAbsent(table, (x) ->
                new MemTable(table, keyFactory, commitLog, mergeStrategy));
    }
}
