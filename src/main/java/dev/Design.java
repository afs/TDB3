/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev;

public class Design {

    /**
     * Triples layout.
     *
     * Quad layout.
     *
     * Prefixes layout.
     *
     * Nodes layout
     */

//    // Ingestion:
//    {
//        Options options;
//        SstFileWriter sst_file_writer = new SstFileWriter(null, options, null)    //(EnvOptions(), options, options.comparator);
//        Status s = sst_file_writer.Open(file_path);
//        assert(s.ok());
//
//        // Insert rows into the SST file, note that inserted keys must be
//        // strictly increasing (based on options.comparator)
//        for (...) {
//            s = sst_file_writer.Add(key, value);
//            assert(s.ok());
//        }
//
//        // Ingest the external SST file into the DB
//        s = db_->IngestExternalFile({"/home/usr/file1.sst"}, IngestExternalFileOptions());
//        assert(s.ok());
//    }

// Adam: 07/03/2020
//    > Q for you - I recently redid my Jena/RockDB store in Jena's new
//    > triplestore architecture.  It all works functionally including the
//    > transactions - thank you!
//
//    Sure no problem. You did the hard work ;-)
//
//
//    > It uses column families then stores everything (3 indexes, and a node id
//    > to node string table) in one RockDB.  Is that a good idea?
//
//    I think a single RocksDB instance is probably the right approach, and
//    then use one column family for each distinct data store, i.e. per-key
//    format. For FusionDB we have about 15 Column Families. Transactions
//    and compaction can operate across column families, yet each has its
//    own WAL which gives you better write performance.
//
//
//    >  Its more
//    > work for separate DBs but I find the loading rate isn't as much as I'd
//    > like - that itself isn't a blocker - I can write a special bulk loader
//    > and tune for large inputs (which probably aren't that large by your
//    > standards).
//
//    A lot of RocksDB is about tuning. It's not a topic that I am
//    particularly expert in (yet). If you are using
//    WriteBatch/WriteBatchWithIndex via RocksDB's Transaction class or your
//    own Transaction class, you may find that reading/writing to/from the
//    db via the batch can be slow. Unfortunately every get/put on the batch
//    involves a JNI call, which has some overhead.
//
//    I have some plans afoot to move the buffer backing the RocksJava
//    WriteBatch into Java's memory space instead of C++'s; This is still in
//    the imaginative stage at the moment though.
//
//    Recently, we also did some work to allow not only using byte[] types
//    for key/value operations, but also to allow for the use of ByteBuffer
//    objects. This should be in the latest RocksJava release.
//    With that facility in mind, I am now personally thinking that in
//    FusionDB I will create one or two "oversized" Native ByteBuffer
//    per-Java ThreadLocal, I will then reuse these Native ByteBuffer for
//    all read/write operations on the database. By "oversized" I mean that
//    these buffers have a highly likelihood of being larger than most of
//    the key/values that I will store, probably just 4KB each or so. As the
//    XML we store is deeply decomposed into key/values our tuples are often
//    quite small. If we need to store something larger, we can allocate a
//    new buffer, or resize one of our existing ThreadLocal buffers. Before
//    I do so, I have been working on a benchmark so that I can evaluate the
//    performance before and after such a change. Maybe such an approach
//    would be useful for you also?
//
//    Maybe also of interest to you... just yesterday, I also saw TRocksDB
//    from Toshiba (or what used to be called Toshiba), it's quite
//    interesting as they have reduced Write-Amplification -
//    https://github.com/KioxiaAmerica/trocksdb

}
