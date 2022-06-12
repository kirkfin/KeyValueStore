package kfinley.keyvaluestore;

import org.junit.Test;

import static org.junit.Assert.*;

public class KVStoreTest {

    private final CommitLog failingCommitLogWriter = new CommitLog() {
        @Override
        public void write(String table, MemTableKey key, Modification modification) {
            throw new RuntimeException();
        }
    };

    @Test
    public void putAndDeleteWithMultipleTablesWithDefaultSettings() {
        KVStore kvStore = new KVStore();
        kvStore.put("table1", "key1", "value1");
        kvStore.put("table1", "key1", "value2");
        kvStore.put("table1", "key1", "value2");

        kvStore.put("table2", "key1", "t2:value1");
        kvStore.put("table2", "key1", "t2:value2");
        kvStore.put("table2", "key1", "t2:value2");

        assertEquals("value2", kvStore.get("table1", "key1"));
        assertEquals("t2:value2", kvStore.get("table2", "key1"));

        kvStore.delete("table2", "key1");
        kvStore.delete("table1", "key1");

        assertNull(kvStore.get("table2", "key1"));
        assertNull(kvStore.get("table2", "key1"));
    }

    @Test
    public void putDeleteAndRePut() {
        KVStore kvStore = new KVStore();
        kvStore.put("table1", "key1", "value1");
        assertEquals("value1", kvStore.get("table1", "key1"));

        kvStore.delete("table1", "key1");
        assertNull(kvStore.get("table2", "key1"));

        kvStore.put("table1", "key1", "value2");
        assertEquals("value2", kvStore.get("table1", "key1"));
    }

    @Test
    public void commitLogFailureCausesPutFailure() {
        KVStore kvStore = new KVStore(KVStore.DEFAULT_KEY_FACTORY, KVStore.DEFAULT_MERGE_STRATEGY, failingCommitLogWriter,
                KVStore.MEM_TABLE_DEFAULT_MAX_SIZE_BYTES);

        try {
            kvStore.put("table1", "key1", "value1");
        } catch (ModificationFailureException e) {
            //pass
            return;
        }
        fail();
    }

    @Test
    public void commitLogFailureCausesDeleteFailure() {
        KVStore kvStore = new KVStore(KVStore.DEFAULT_KEY_FACTORY, KVStore.DEFAULT_MERGE_STRATEGY, failingCommitLogWriter,
                KVStore.MEM_TABLE_DEFAULT_MAX_SIZE_BYTES);

        try {
            kvStore.delete("table1", "key1");
        } catch (ModificationFailureException e) {
            //pass
            return;
        }
        fail();
    }
}
