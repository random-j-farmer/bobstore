/*
Package bobstore - a blobstore for small/medium blobs.

It is meant to store a couple of TB
of 10k-50k json blobs, to take pressure
of a non-distributed database.

It uses a directory with max. 64k data
files each 1GB long.  There is one lock file
which is locked exclusively by the
single writing process.

It is safe to use the same opened db handle
from multiple goroutines.  If this is the
writing process, writing from multiple goroutines
is supported.

Blobs are only appended.  They are never
modified, and individual blobs can not be
deleted (whole files can be deleted, just
don't reference their data after deletion).
*/
package bobstore
