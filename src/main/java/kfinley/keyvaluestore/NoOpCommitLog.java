package kfinley.keyvaluestore;

import java.util.logging.Logger;

public class NoOpCommitLog implements CommitLog {

    private final Logger logger = Logger.getLogger(getClass().getName());

    @Override
    public void write(String table, MemTableKey key, Modification modification) {
        logger.warning("Commit log disabled: table=" + table + ",key=" + key.getKey() + ",value=" + modification.getModification());
    }
}
