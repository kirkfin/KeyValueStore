package kfinley.keyvaluestore;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/***
 * Single threaded MemTable implementation: PUT/GET/DELETE operations are synchronous on the local worker thread
 * - This class provides `getSizeBytes` so that capacity flushes may be triggered externally by calling `triggerFlush`
 * - TODO: Implement CommitLog and SSTable
 */
public class MemTable {

    private final String table;
    private final MemTableKeyFactory keyFactory;
    private final CommitLog commitLog;
    private final ModificationMergeStrategy mergeStrategy;
    private final AtomicLong currentTableSizeBytes = new AtomicLong();
    private final ExecutorService flushThread = Executors.newSingleThreadExecutor();
    private final ExecutorService workerThread = Executors.newSingleThreadExecutor();
    private final Lock flushLock = new ReentrantLock();
    private SortedMap<MemTableKey, TreeSet<Modification>> tableModifications = new TreeMap<>();

    public MemTable(String table,
                    MemTableKeyFactory keyFactory,
                    CommitLog commitLog,
                    ModificationMergeStrategy mergeStrategy) {
        this.table = table;
        this.keyFactory = keyFactory;
        this.commitLog = commitLog;
        this.mergeStrategy = mergeStrategy;
    }

    public void put(String key, String value) {
        MemTableKey keyRef = keyFactory.getKey(key);
        try {
            executePut(keyRef, value);
        } catch (Exception e) {
            throw new ModificationFailureException("Failed trying to put key=" + key + ",value=" + value, e);
        }
    }


    public String get(String key) {
        MemTableKey keyRef = keyFactory.getKey(key);
        try {
            return executeGet(keyRef);
        } catch (Exception e) {
            throw new ModificationFailureException("Failed trying to get key=" + key, e);
        }
    }

    public void delete(String key) {
        MemTableKey keyRef = keyFactory.getKey(key);
        try {
            executeDelete(keyRef);
        } catch (Exception e) {
            throw new ModificationFailureException("Failed trying to delete key=" + key, e);
        }
    }


    public long getSizeBytes() {
        return currentTableSizeBytes.get();
    }


    public String executeGet(MemTableKey key) throws ExecutionException, InterruptedException {
        return workerThread.submit(() -> {
            TreeSet<Modification> modifications = tableModifications.get(key);
            if (modifications == null) {
                return null;
            }
            Modification modification = mergeStrategy.merge(modifications);
            if (modification == null) {
                return null;
            }
            if(modification.getModification() == null){
                return SSTable.lookup(table, key); // Try the SSTable if not found in the cache
            }
            return modification.getModification();
        }).get();
    }

    public void executePut(MemTableKey key, String value) throws ExecutionException, InterruptedException {
        executePut(key, new Modification(value));
    }

    public void executeDelete(MemTableKey key) throws ExecutionException, InterruptedException {
        executePut(key, new Modification(null));
    }

    private void executePut(MemTableKey key, Modification modification) throws ExecutionException, InterruptedException {
        workerThread.submit(() -> {

            commitLog.write(table, key, modification); // Write to the commit log first

            if (tableModifications.computeIfPresent(key, (x, modifications) -> {
                modifications.add(modification);
                currentTableSizeBytes.addAndGet(modification.getSizeBytes());
                return modifications;
            }) == null) {
                currentTableSizeBytes.addAndGet(key.getKey().length() + modification.getSizeBytes());
                tableModifications.put(key, new TreeSet<>() {{
                    add(modification);
                }});
            }
        }).get();
    }

    /***
     * Trigger a flush of the local cache, storing the modifications as a new SSTable on disk
     */
    public Future<SortedMap<MemTableKey, Modification>> triggerFlush() {
        if (flushLock.tryLock()) {
            try {
                SortedMap<MemTableKey, TreeSet<Modification>> flushedBatch = flush();
                return flushThread.submit(() -> executeFlush(flushedBatch));
            } finally {
                flushLock.unlock();
            }
        }
        return null;
    }

    /***
     * Merge modifications and trigger SSTable flush
     */
    private SortedMap<MemTableKey, Modification> executeFlush(SortedMap<MemTableKey, TreeSet<Modification>> flushedBatch) {
        SortedMap<MemTableKey, Modification> flattenedModifications = new TreeMap<>();
        for (Map.Entry<MemTableKey, TreeSet<Modification>> entry : flushedBatch.entrySet()) {
            flattenedModifications.put(entry.getKey(), mergeStrategy.merge(entry.getValue()));
        }
        SSTable ssTable = new SSTable(table, flattenedModifications);
        ssTable.flush();
        return flattenedModifications;
    }

    /***
     * Return all table modifications and reset the local cache
     */
    private SortedMap<MemTableKey, TreeSet<Modification>> flush() {
        currentTableSizeBytes.set(0);
        SortedMap<MemTableKey, TreeSet<Modification>> modifications = new TreeMap<>(tableModifications);
        this.tableModifications = new TreeMap<>();
        return modifications;
    }
}
