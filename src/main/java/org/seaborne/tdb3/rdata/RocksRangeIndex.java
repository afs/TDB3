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

import java.nio.ByteBuffer;
import java.util.ArrayList ;
import java.util.Iterator ;
import java.util.List ;

import org.apache.jena.atlas.iterator.Iter ;
import org.apache.jena.atlas.lib.Bytes ;
import org.apache.jena.dboe.base.record.Record ;
import org.apache.jena.dboe.base.record.RecordFactory ;
import org.apache.jena.dboe.base.record.RecordMapper ;
import org.apache.jena.dboe.index.RangeIndex ;
import org.apache.jena.tdb2.TDBException ;
import org.rocksdb.*;
import org.seaborne.tdb3.sys.BuildR;

public class RocksRangeIndex implements RangeIndex, RocksPrepare {

    private final byte[] empty = new byte[0];
    private final RocksDB db;
    private final RecordFactory factory ;
    private final ColumnFamilyHandle colFamily ;

    public RocksRangeIndex(RocksDB db, ColumnFamilyHandle colFamily, RecordFactory factory) {
        this.db = db;
        this.factory = factory;
        this.colFamily = colFamily;
    }

    @Override
    public Record find(Record record) {
        try {
            // Direct to record value?
            byte[] v = db.get(colFamily, record.getKey()) ;
            if ( v == null )
                return null;
            return getRecordFactory().create(record.getKey(), v);
        }
        catch (RocksDBException e) {
            throw new TDBException(e);
        }
    }

    @Override
    public boolean contains(Record record) {
        try {
            //keyMayExist
            byte[] v = db.get(colFamily, record.getKey()) ;
            return v != null ;
        }
        catch (RocksDBException e) {
            throw new TDBException(e);
        }
    }

    //CRUDE batching to see if it makes a difference.

    private int i = 0 ;
    private WriteBatch writeBatch = new WriteBatch();
    private WriteOptions writeOptions = new WriteOptions();

    @Override
    public boolean insert(Record record) {
        try {
            byte[] v = record.getValue();
            if ( v == null )
                v = empty;
//            byte[] v0 = db.get(colFamily, record.getKey());
//            if ( v0 != null ) {
//                if ( Bytes.compare(v, v0) == 0 )
//                    return false;
//            }

            if ( BuildR.batchSizeNodeTable > 0 ) {
                //XXX writeBatch.put(colFamily,ByteBuffer,ByteBuffer);
                writeBatch.put(colFamily, record.getKey(), v) ;
                i++;
                flushBatch(BuildR.batchSizeIndex);
            } else
                db.put(colFamily, record.getKey(), v) ;
            return true;
        }
        catch (RocksDBException e) {
            throw new TDBException(e);
        }
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
    public boolean delete(Record record) {
        try {
//            byte[] v0 = db.get(colFamily, record.getKey());
//            if ( v0 == null )
//                return false;

            // WriteBatch
            //writeBatch.delete();
            db.delete(colFamily, record.getKey());
            return true;
        }
        catch (RocksDBException e) {
            throw new TDBException(e);
        }
    }

    @Override
    public <X> Iterator<X> iterator(Record recordMin, Record recordMax, RecordMapper<X> mapper) {
        // XXX Revisit!
        try (RocksIterator iter = db.newIterator(colFamily)) {
            if ( recordMin == null )
                iter.seekToFirst();
            else
                iter.seek(recordMin.getKey());
            List<X> x = new ArrayList<>(1000);
            while(iter.isValid()) {
                byte[]k = iter.key();
                if ( recordMax != null ) {
                    if ( Bytes.compare(k, recordMax.getKey()) >= 0 )
                        break;
                }
                byte[]v = iter.value();
                //x.add(getRecordFactory().create(k,v));
                X mx = mapper.map(ByteBuffer.wrap(k), 0, k, getRecordFactory());
                x.add(mx);
                iter.next();
            }
            return x.iterator();
        }
    }

    @Override
    public Iterator<Record> iterator() {
        return iterator(null, null);
    }

    // Chunking version.
    @Override
    public Iterator<Record> iterator(Record recordMin, Record recordMax) {
        try (RocksIterator iter = db.newIterator(colFamily)) {
            if ( recordMin == null )
                iter.seekToFirst();
            else
                iter.seek(recordMin.getKey());
            List<Record> x = new ArrayList<>(1000);
            while(iter.isValid()) {
                byte[]k = iter.key();
                if ( recordMax != null ) {
                    if ( Bytes.compare(k, recordMax.getKey()) >= 0 )
                        break;
                }
                byte[]v = iter.value();
                x.add(getRecordFactory().create(k,v));
                iter.next();
            }
            return x.iterator();
        }
    }

    @Override
    public RecordFactory getRecordFactory() {
        return factory ;
    }

    @Override
    public void close() {}

    @Override
    public boolean isEmpty() {
        try (RocksIterator iter = db.newIterator(colFamily)) {
            iter.seekToFirst();
             return ! iter.isValid();
        }
    }

    @Override
    public void clear() {
        // TODO better!
        try {
            // drop ColumnFamilyDescriptor
            db.deleteRange(colFamily, new byte[] {0}, new byte[] { (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF});
        }  catch (RocksDBException e) {
            throw new TDBException(e);
        }
    }

    @Override
    public void check() {}

    @Override
    public long size() {
        // Inefficient!
        return Iter.count(iterator());
    }

    @Override
    public void sync() {}

    @Override
    public Record minKey() {
        try (RocksIterator iter = db.newIterator(colFamily)) {
            iter.seekToFirst();
            if ( !iter.isValid() )
                return null ;
            byte[] k = iter.key();
            byte[] v = iter.value();
            return getRecordFactory().create(k, v);
        }
    }

    @Override
    public Record maxKey() {
        try (RocksIterator iter = db.newIterator(colFamily)) {
            iter.seekToLast();
            if ( !iter.isValid() )
                return null ;
            byte[] k = iter.key();
            byte[] v = iter.value();
            return getRecordFactory().create(k, v);
        }
    }
}