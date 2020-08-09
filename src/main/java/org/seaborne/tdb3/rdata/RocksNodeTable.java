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

package org.seaborne.tdb3.rdata;

import java.util.Iterator ;

import org.apache.jena.atlas.lib.Bytes ;
import org.apache.jena.atlas.lib.InternalErrorException ;
import org.apache.jena.atlas.lib.Pair ;
import org.apache.jena.dboe.index.Index ;
import org.apache.jena.graph.Node ;
import org.apache.jena.tdb2.TDBException ;
import org.apache.jena.tdb2.store.NodeId ;
import org.apache.jena.tdb2.store.NodeIdFactory ;
import org.apache.jena.tdb2.store.nodetable.NodeTableNative ;
import org.rocksdb.* ;
import org.seaborne.tdb3.ByteCodec;
import org.seaborne.tdb3.RocksException;
import org.seaborne.tdb3.sys.BuildR;

public class RocksNodeTable extends NodeTableNative implements RocksPrepare {
    private final ByteCodec<Node> codec ;
    private final RocksDB db ;
    private final ColumnFamilyHandle columnFamily;

    /* How often to save the id -
     * This reduces the number of JNI writes
     * but requires start to scan to recover the last.
     */
    private static int IdAllocSaveTick = 20;

    // Id allocation. Sequential but don't write each time.
    // Instead, update every "tick" new nodes and recover by
    // finding the recorded sequence number and reading forwards
    // until the nodes sequence ends.

    // Id of the first node allocated. Positive number.
    // Not zero - that looks like uninitialized bytes and may clash with nextIdKey.
    private static long initialNextId = 1;

    // This must not be a valid NodeId byte form!
    // (NodeId high bit is 1 => inline value).
    // Either -1, which is too big as an unsigned long
    // or 0 which is too small because we don't use id = 0
    // or something the wrong length.
    // The restart code copies with all cases but this is persistsed.
    private static final byte[] nextIdKey = {0};

    private final byte[] nextIdBytes = new byte[8];
    private long nextId = 1;

    public RocksNodeTable(RocksDB db, ColumnFamilyHandle columnFamily, Index nodeToId, ByteCodec<Node> codec) {
        super(nodeToId) ;
        this.codec = codec;
        this.columnFamily = columnFamily;
        this.db = db;
        // Get the id start.
        try {
            int rc = db.get(columnFamily, nextIdKey, nextIdBytes);
            if ( rc == RocksDB.NOT_FOUND ) {
                //System.err.println("Init counter");
                // Initalize.
                nextId = initialNextId;
            } else {
                // Scan forward.
                long x = Bytes.getLong(nextIdBytes);
                //System.err.println("Recover counter : "+x);
                long y = scanFrom(nextIdBytes);
                long y0 = scanFrom0(nextIdBytes);
                //System.err.println("Last id :    "+y);
                if ( y != y0 )
                    throw new InternalErrorException();

                if ( y == -1) {
                    // No other key found - the nextIdKey was the latest.
                    nextId = x;
                } else {
                    // Set to one more than last key found.
                    nextId = y+1;
                }
            }
            //System.err.println("Set counter : "+nextId);
            Bytes.setLong(nextId, nextIdBytes);
            db.put(columnFamily, nextIdKey, nextIdBytes);
        }
        catch (RocksDBException e) { throw RocksException.wrap(e); }
    }

    /** Scan to find the largest in-use id.
     * Retrun -1 for no key found. */
    private long scanFrom(byte[] startBytes) {
        RocksIterator iter = db.newIterator(columnFamily);
        iter.seek(startBytes);
        // The "next id" key may be at the top of the key range, so scan carefully.
        long seen = -1;
        while(iter.isValid()) {
            byte[] k = iter.key();
            // nextIdKey reached. Ignore.
            if ( Bytes.compare(nextIdKey, k) == 0 )
                break;
            seen = Bytes.getLong(k);
            iter.next();
        }
        return seen ;
    }

    /* Scan one-by-one, not as good because a small gap will stop the scan.
     * By tolerating gaps (in the iterator version) we can write the key state
     * before the key itself which may miss in the event of a crash.
     * (The code actualy writes after a key is used.)
     */
    private long scanFrom0(byte[] startBytes) throws RocksDBException {
        //Scan one-by-one.
        long probe = Bytes.getLong(startBytes);
        long seen = -1;
        for(;;) {
            Bytes.setLong(probe, key);
            byte[] bytes = db.get(columnFamily,key);
            if ( bytes == null )
                break;
            seen = probe;
            probe = probe+1;
        }
        return seen ;
    }

    // bytes <-> Node.
    // Methods writeNodeToTable and readNodeFromTable are called inside synchronized in NodeTableNative.

    // Workspace building the key.
    private final byte[] key = new byte[8];

    private int i = 0;
    private WriteBatch writeBatch = new WriteBatch();
    private WriteOptions writeOptions = new WriteOptions();

    @Override
    protected NodeId writeNodeToTable(Node node) {
        // Alloc key.
        long id = nextId++;
        try {
            Bytes.setLong(id, key);
            byte[] bytes = codec.encode(node);

            if ( BuildR.batchSizeNodeTable > 0 ) {
                writeBatch.put(columnFamily, key, bytes);
                i++;
                flushBatch(BuildR.batchSizeNodeTable);
            }
            else
                db.put(columnFamily, key, bytes);

            // Record the counter every so often.
            if ( nextId % IdAllocSaveTick == 0 ) {
                Bytes.setLong(nextId, nextIdBytes);
                db.put(columnFamily, nextIdKey, nextIdBytes);
            }
        } catch (Exception e) { throw new TDBException(e); }
        return NodeIdFactory.createPtr(id);
    }

    private void flushBatch(int limit) {
        if ( i > limit ) {
            try {
                db.write(writeOptions, writeBatch);
                writeBatch.clear();
                i = 0;
            }
            catch (RocksDBException e) {
                throw new TDBException(e);
            }
        }
    }

    @Override
    public void prepare() {
        flushBatch(0);
    }

    @Override
    protected Node readNodeFromTable(NodeId id) {
        NodeIdFactory.set(id, key);
        try {
            byte[] v = db.get(columnFamily, key);
            if ( v == null )
                return null;
            return codec.decode(v);
        }
        catch (Exception e) {
            throw new TDBException(e);
        }
    }

    @Override
    public Iterator<Pair<NodeId, Node>> all() {
        return super.all();
    }

    @Override
    protected void syncSub() {
        try ( FlushOptions options = new FlushOptions() ) {
            options.setWaitForFlush(true);
            db.flush(options);
        } catch (Exception e) {
            throw new TDBException(e);
        }
    }

    @Override
    protected void closeSub() {
    }

}
