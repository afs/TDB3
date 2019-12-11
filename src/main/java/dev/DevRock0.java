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

public class DevRock0 {
    
    
    // Old notes
    /* Issues
     * StorageRDF
     * Close DB
     *   Add RocksDB to DatasetGraphTDB some how.
     *    (add Closable?)
     * 
     * [1] insert() vs insertIfAbsent() // delete() vs deleteIfPresent()
     * [2] Slicing iterators
     * [3] Avoid creating byte[]- recycle - add to API for get
     * [4] ?? multiGet
     * [5] codec for Node <-> byte[], including streaming
     *    Faster than thrift.?
     * [6] R_RI - find -> Direct to record (assumes set).
     * [7] Byte codec and .... Nodec 
     * 
     * Check all operations implemented R_*
     * 
     * Avoid checking for exists.
     *   DatasetGraph.compact()
     *   Batching iterator.
     *   Avoid Records? RangeIndex<X,Y>
     *      But records prepare the K/V.
     * 
     * Codec and term recycling.
     *   INFO Finished: 5,000,599 bsbm-5m.nt.gz 97.99s (Avg: 51,033)
     *   INFO  TDB3                 :: Finished: 24,997,044 bsbm-25m.nt.gz 660.77s (Avg: 37,830)   
     *   INFO  TDB3                 :: Finished: 24,997,044 bsbm-25m.nt.gz 630.73s (Avg: 39,632)
     * BSBM 25m load.
     *   Without contains test
     * INFO  Finished: 24,997,044 bsbm-25m.nt.gz 677.83s (Avg: 36,878)
     * INFO  Finished: 5,000,599 bsbm-5m.nt.gz 104.61s (Avg: 47,800)
     * Without putting the count:
     *   INFO  Finished: 5,000,599 bsbm-5m.nt.gz 99.33s (Avg: 50,343)
     *   With contains test: 
     * INFO  Finished: 5,000,599 bsbm-5m.nt.gz 171.84s (Avg: 29,100)

     * TDB2:
     * INFO  Finished: 24,997,044 bsbm-25m.nt.gz 379.98s (Avg: 65,784)
     * 
     * RangeIndex:
     * public Iterator<Record> iterator(Record recordMin, Record recordMax) {
     * 
     * Key only version.
     * RangeIndex<X>
     * public Iterator<X> iterator(X recordMin, X recordMax) {
     * TupleIndex
     *   Fast Tuple<NodeId>, length 3 or 4.
     *   Tuple3X as overlay of bytes? Zero copy at retrieve time but reconstruction costs each time.
     *     vs copy out to int/long once, may be able to reuse byte[] in iterator.
     */
}
