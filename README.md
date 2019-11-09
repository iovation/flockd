flockd 0.3.0
============

[![Build Status](https://travis-ci.org/iovation/flockd.svg)](https://travis-ci.org/iovation/flockd)
[![Coverage Status](https://coveralls.io/repos/github/iovation/flockd/badge.svg)](https://coveralls.io/github/iovation/flockd)
[![GoDoc](https://godoc.org/github.com/iovation/flockd?status.svg)](https://godoc.org/github.com/iovation/flockd)
[![License](https://img.shields.io/github/license/iovation/flockd.svg)](https://github.com/iovation/flockd/blob/master/LICENSE.md)

flockd provides a simple file system-based key/value database that uses file
locking for concurrency safety. Keys correspond to files, values to their
contents, and tables to directories. Files are share-locked on read (Get and
ForEach) and exclusive-locked on write (Set, Create, Update, and Delete).

This may be overkill if you have only one application using a set of files in a
directory. But if you need to sync files between multiple systems, like a
distributed database, assuming your sync software respects file system locks,
flockd might be a great way to go. This is especially true for modestly-sized
databases and databases with a single primary instance and multiple read-only
secondary instances.

In any event, your file system must support proper file locking for this to
work. If your file system does not, it might still work if file renaming and
unlinking is atomic and flockd is used exclusively to access files. If not, then
all bets are off, and you can expect occasional bad reads.

All of this may turn out to be a bad idea. YMMV. Warranty not included.

Inspirations
------------

*   [diskv](https://github.com/peterbourgon/diskv): Similar use of one file per
    key/value pair. Uses a sync.RWMutex for concurrency protection. Lots of
    features, including path transformation, caching, and compression.

*   [fskv](https://github.com/nickalie/fskv): Use of buckets similar to Tables
    here (I stole the idea, really). Relies on temporary files for locking.
    Depends on [afero](https://github.com/spf13/afero) for file management.

*   [Scribble](https://github.com/nanobox-io/golang-scribble): Uses a single
    JSON file for the database. Relies on sync.Mutex for concurrency control.
