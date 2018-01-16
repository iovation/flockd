flockd 0.1.0
============

flockd provides a simple file system-based key/value database that uses file
locking for concurrency safety. Keys correpond to files, values to their
contents, and tables to directories.

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
