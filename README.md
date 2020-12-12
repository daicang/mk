# mk

*M*inimal, runable *K*-v storage, for learning and fun.

## Introduction

mk intends to be a minimal k-v storage using b+tree and mmap. It is mainly inspired by [BoltDB](https://github.com/boltdb/bolt), [lmdb](https://github.com/LMDB/lmdb) and [btree](https://github.com/google/btree)
Different from BoltDB, mk doesn't support bucket.

## Operations

mk supports set/get operations

## Storage layout

DB file has at least 4 pages on disk. First 2 pages are meta page, then 1 free

## Transaction

mk supports multiple operations in one transaction. B+tree rebalancing and sync to disk are delayed until end of each transaction.


## Audit

Audit is not implemented yet, have plan to add soon.
