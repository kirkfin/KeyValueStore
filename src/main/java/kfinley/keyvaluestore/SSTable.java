package kfinley.keyvaluestore;

import java.util.SortedMap;

/***
 * Not implemented
 * - A disk based, LSM-Tree index for storing and looking up Strings in sorted order
 */
public class SSTable {

    public SSTable(String table, SortedMap<MemTableKey, Modification> tableModifications) {

    }

    public void flush() {

    }

    public static String lookup(String table, MemTableKey key){
        return null;
    }
}
