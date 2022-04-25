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

package org.seaborne.tdb3.sys;

import static java.nio.charset.StandardCharsets.US_ASCII ;

import java.nio.charset.StandardCharsets ;
import java.util.ArrayList ;
import java.util.HashMap ;
import java.util.List ;
import java.util.Map ;

import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.lib.InternalErrorException ;
import org.apache.jena.dboe.base.file.Location ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.tdb2.params.StoreParams;
import org.rocksdb.* ;
import org.seaborne.tdb3.RocksException;

// XXX -> Const
// XXX Merge with TDBBuilderR
public class BuildR {
    // Hash to NodeId

    private static StoreParams p = StoreParams.getDftStoreParams();


    // XXX
//    public static ColumnFamilyOptions colFamilyOpts(ColumnFamilyDescriptor descr) {
//        ColumnFamilyOptions cfo = new ColumnFamilyOptions();
//        return cfo;
//    }

    public static ColumnFamilyDescriptor cfNodeHash2Id = columnFamily(p.getNodeTableBaseName()+":NodeHash2Id");
    public static ColumnFamilyDescriptor cfNodeId2Node = columnFamily(p.getNodeTableBaseName()+":NodeId2Node");

    public static ColumnFamilyDescriptor cfPrefixNodeHash2Id = columnFamily(p.getPrefixTableBaseName()+":NodeHash2Id");
    public static ColumnFamilyDescriptor cfPrefixNodeId2Node = columnFamily(p.getPrefixTableBaseName()+":NodeId2Node");

    public static Map<String, ColumnFamilyDescriptor> families = new HashMap<>();

    public static ColumnFamilyDescriptor cfSPO = columnFamily("SPO");
    public static ColumnFamilyDescriptor cfPOS = columnFamily("POS");
    public static ColumnFamilyDescriptor cfOSP = columnFamily("OSP");

    public static ColumnFamilyDescriptor cfGSPO = columnFamily("GSPO");
    public static ColumnFamilyDescriptor cfGPOS = columnFamily("GPOS");
    public static ColumnFamilyDescriptor cfGOSP = columnFamily("GOSP");

    public static ColumnFamilyDescriptor cfSPOG = columnFamily("SPOG");
    public static ColumnFamilyDescriptor cfPOSG = columnFamily("POSG");
    public static ColumnFamilyDescriptor cfOSPG = columnFamily("OSPG");

    public static ColumnFamilyDescriptor cfGPU = columnFamily("GPU");

    public static ColumnFamilyDescriptor[] columnFamilies = {cfNodeHash2Id, cfNodeId2Node,
                                                             cfPrefixNodeHash2Id, cfPrefixNodeId2Node,
                                                             cfSPO, cfPOS, cfOSP,
                                                             cfGSPO, cfGPOS, cfGOSP,
                                                             cfSPOG, cfPOSG, cfOSPG,
                                                             cfGPU
    };


    // -1 : off.
    public static int batchSizeIndex = -1;
    public static int batchSizeNodeTable = -1;

    // Note to self... again ... after all constants
    static { init() ; }
    private static void init() {
        for(ColumnFamilyDescriptor cfDesc : columnFamilies ) {
            String name = new String(cfDesc.getName(), US_ASCII);
            families.put(name, cfDesc);
        }
    }

    private static ColumnFamilyDescriptor columnFamily(String name) {
        return new ColumnFamilyDescriptor(name.getBytes(US_ASCII));
    }

    public interface RocksAction { void run() throws RocksDBException; }

    /** RocksDB transaction */
    // Wild guess at the style!
    public static void rtx(RocksDB rdb, RocksAction action) {
        try {
            action.run();
        } catch (RocksDBException ex) { throw RocksException.wrap(ex); }
    }

    public static RocksTDB openDB(String db_path) {
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        columnFamilyDescriptors.addAll(families.values());
        // have to open default column family
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
        RocksTDB rtdb = openDB(db_path, columnFamilyDescriptors);
        return rtdb;
    }

    private static RocksTDB openDB(String db_path, List<ColumnFamilyDescriptor> columnFamilyDescriptors) {
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        // leak?
        try {
            BlockBasedTableConfig tfc = new BlockBasedTableConfig();
            Cache cache = new LRUCache(100*1024*1024); // Dft: 8M
            tfc.setBlockCache(cache);
            tfc.setBlockSize(32*1024);

            // Also:

            Options options = new Options();
            options.setTableFormatConfig(tfc);
            options.setCompactionStyle(CompactionStyle.LEVEL);
            options.setAllowMmapReads(true);
            options.setAllowMmapWrites(true);
            options.setIncreaseParallelism(3);

            CompressionOptions compressionOptions = new CompressionOptions();
            compressionOptions.setEnabled(true);
            options.setCompressionOptions(compressionOptions);

            DBOptions dbOptions = new DBOptions(options);
            dbOptions.setCompactionReadaheadSize(2*1024*1024);
            dbOptions.setCreateIfMissing(true);
            dbOptions.setUnorderedWrite(true);

            //dbOptions.createMissingColumnFamilies()
            //dbOptions.setTwoWriteQueues(true);

            dbOptions.setMaxBackgroundJobs(3);

//            TransactionDBOptions x = new TransactionDBOptions();
////            x.setNumStripes(0);
////            x.setDefaultLockTimeout(0);
////            x.setTransactionLockTimeout(0);
//            x.setWritePolicy(TxnDBWritePolicy.WRITE_PREPARED);

//            TransactionDB db = TransactionDB.open(dbOptions, x, db_path, columnFamilyDescriptors, columnFamilyHandles) ;
////            MutableDBOptionsBuilder opt2b = MutableDBOptions.builder()
////                .setWritableFileMaxBufferSize(10*1024*1024)
////                .setCompactionReadaheadSize(2*1024*1024)
////                .setMaxOpenFiles(-1)
////                ;
////            opt2b.setBaseBackgroundCompactions(2);
////            db.setDBOptions(opt2b.build());

//            OptimisticTransactionOptions x1 = new OptimisticTransactionOptions();
            OptimisticTransactionDB db =
                    OptimisticTransactionDB.open(dbOptions, db_path, columnFamilyDescriptors, columnFamilyHandles) ;

            RocksTDB rtdb = new RocksTDB(db, Location.create(db_path), columnFamilyDescriptors, columnFamilyHandles);
            return rtdb;
        } catch (RocksDBException ex) { throw RocksException.wrap(ex); }
    }

    public static void formatDatabase(String loc) {

        try (final Options options = new Options();
            final Filter bloomFilter = new BloomFilter(10);
            final ReadOptions readOptions = new ReadOptions();
            final Statistics stats = new Statistics();
            final RateLimiter rateLimiter = new RateLimiter(10000000,10000, 10)) {

//            options.setIncreaseParallelism(4);
//            options.setAllowMmapReads(true);
//            options.setAllowMmapWrites(true);
//            options.setWriteBufferSize(16*1024*1024);

            options.setCreateIfMissing(true);
            readOptions.setFillCache(false);

            try(RocksDB rdb = RocksDB.open(options, loc) ) {
                rtx(rdb, ()->{
                    for(ColumnFamilyDescriptor cfDesc : columnFamilies ) {
                        rdb.createColumnFamily(cfDesc);
                    }
                });
            }
        } catch (RocksDBException ex) { throw RocksException.wrap(ex); }
    }


    public static DatasetGraph build(String dirname) {
        FileOps.ensureDir(dirname);
        boolean empty = ! FileOps.existsAnyFiles(dirname);
        if ( empty )
            formatDatabase(dirname);
        RocksTDB rtdb = openDB(dirname);
//        if ( ! empty )
//            try {
//                rtdb.rdb.compactRange();
//            }
//            catch (RocksDBException e) {
//                throw RocksException.wrap(e);
//            }
        Location location = Location.create(dirname);
        System.err.println("RE-ENABLE");
        return null;
        //return TDBBuilderR.build(rtdb);
    }

    public static ColumnFamilyHandle findHandle(RocksTDB db, String columnFamilyDescriptorName) {
        ColumnFamilyDescriptor columnFamilyDescriptor = columnFamily(columnFamilyDescriptorName);
        return findHandle(db, columnFamilyDescriptor);
    }

    public static ColumnFamilyHandle findHandle(RocksTDB db, ColumnFamilyDescriptor columnFamilyDescriptor) {
        String n = new String(columnFamilyDescriptor.getName(), StandardCharsets.US_ASCII);
        ColumnFamilyDescriptor cfd = BuildR.families.get(n);
        int i = db.descriptors.indexOf(cfd);
        if ( i < 0 )
            throw new InternalErrorException();

        ColumnFamilyHandle h = db.cfHandles.get(i);
        return h ;
    }


}
