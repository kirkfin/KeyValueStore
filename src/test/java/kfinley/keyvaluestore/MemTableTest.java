package kfinley.keyvaluestore;

import org.junit.Before;
import org.junit.Test;


import java.util.SortedMap;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MemTableTest {

    private MemTable table;

    @Before
    public void setup() {
        table = new MemTable("test", KVStore.DEFAULT_KEY_FACTORY, KVStore.DEFAULT_COMMIT_LOG, KVStore.DEFAULT_MERGE_STRATEGY);
    }

    @Test
    public void singleWorkerPutAndDelete() {
        table.put("key1", "value1");
        table.put("key1", "value2");
        table.put("key1", "value3");

        table.put("key2", "k2:value1");

        assertEquals("value3", table.get("key1"));
        assertEquals("k2:value1", table.get("key2"));
        assertNull(table.get("not-present"));

        table.delete("key1");
        table.delete("key2");
        assertNull(table.get("key1"));
        assertNull(table.get("key2"));
    }

    @Test
    public void twoWorkersMultiplePuts() {
        table.put("key1", "value1");
        table.put("key1", "value2");
        table.put("key1", "value3");

        table.put("key2", "k2:value1");

        assertEquals("value3", table.get("key1"));
        assertEquals("k2:value1", table.get("key2"));
        assertNull(table.get("not-present"));

        table.delete("key1");
        table.delete("key2");
        assertNull(table.get("key1"));
        assertNull(table.get("key2"));
    }

    @Test
    public void twoWorkersTriggerFlush() throws ExecutionException, InterruptedException {
        table.put("key2", "k2:value1");
        table.put("key1", "value1");
        table.put("key1", "value2");
        table.put("key1", "value3");

        SortedMap<MemTableKey, Modification> flushed = table.triggerFlush().get();
        assertEquals(2, flushed.size());

        assertEquals("key1", flushed.firstKey().getKey());
        assertEquals("key1", flushed.firstKey().getKey());
        assertEquals("value3", flushed.get(flushed.firstKey()).getModification());
        assertEquals("k2:value1", flushed.get(flushed.lastKey()).getModification());
    }
}
