package kfinley.keyvaluestore;

import java.util.SortedSet;

/***
 * Determines how an ordered set of modifications should be merged into a single modification
 */
public interface ModificationMergeStrategy {
    Modification merge(SortedSet<Modification> modifications);
}
