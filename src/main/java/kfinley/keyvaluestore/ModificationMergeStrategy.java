package kfinley.keyvaluestore;

import java.util.SortedSet;

public interface ModificationMergeStrategy {
    Modification merge(SortedSet<Modification> modifications);
}
