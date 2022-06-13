## A Cassandra-style, single node, key/value store implementation 
* Write flow:
*       1) Write modification to CommitLog
*       2) Write modifications MemTable cache
*       3) When MemTable exceeds capacity, flush modifications to SSTable
* Read flow:
*       1) Check MemTable cache
*       2) Check SSTable

## TODO: Implement SSTable and CommitLog