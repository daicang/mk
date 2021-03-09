<p align="center">
    <b>mk</b><br>
    <i>M</i>inimal <i>K</i>ey-value storage<br>
    <img src="https://raw.githubusercontent.com/daicang/mk/dev/mk.png" width="200">
</p>

---

## Introduction

mk intends to be a minimal runable k-v storage using b+tree and mmap, for learning & fun. Mk is mainly inspired by [BoltDB](https://github.com/boltdb/bolt), [lmdb](https://github.com/LMDB/lmdb) and [btree](https://github.com/google/btree)

## Operations

- set/get/remove
- transaction. Only one writable transaction is allowed at one time
- unlike boltdb, bucket is not supported in mk

## Indexing and storage

- b+tree indexing
- mmap-based storage, single file on disk

## Todos

- Audit
- Visualization
