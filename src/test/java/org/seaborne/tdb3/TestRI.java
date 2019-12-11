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

package org.seaborne.tdb3;

public class TestRI {}

//public class TestRI extends AbstractTestRangeIndex {
//
//    static { 
//        LogCtl.setJavaLogging(); 
//        try { LogCtl.setLog4j(); } catch (Exception ex) {} 
//        FileOps.ensureDir("TEST");
//    }
//    static RocksDB db = null;
//    static ColumnFamilyHandle cfh = null;
//
//    static final String COL_NAME = "NAME" ;
//    static final String DB_NAME = "TEST" ;
//
//    static boolean scopeClass = true ;
//
//    @BeforeClass public static void beforeClass() {
//        FileOps.clearAll(DB_NAME);
//        try (final Options options = new Options();
//            final Filter bloomFilter = new BloomFilter(10);
//            final ReadOptions readOptions = new ReadOptions();
//            final Statistics stats = new Statistics();
//            final RateLimiter rateLimiter = new RateLimiter(10000000,10000, 10)) {
//            options.setCreateIfMissing(true);
//            readOptions.setFillCache(false);
//            ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(COL_NAME.getBytes(StandardCharsets.US_ASCII));
//
//            try (RocksDB dbInit = RocksDB.open(options, DB_NAME)) {
//                ColumnFamilyHandle cfhInit = dbInit.createColumnFamily(cfd);
//            }
//        }
//        catch (RocksDBException e) {
//            e.printStackTrace();
//        }
//
//        if ( scopeClass )
//            openDB();
//    }
//
//    @AfterClass public static void afterClass() {
//        if ( scopeClass )
//            closeDB();
//    }
//
//    @Before
//    public void beforeTest() {
//        if ( !scopeClass )
//            openDB() ;
//    }
//
//    @After
//    public void afterTest() {
//        if ( !scopeClass )
//            closeDB() ;
//    }
//
//    private static void openDB() {
//        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
//        // have to open default column family
//        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
//        // open the new one, too
//        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(COL_NAME.getBytes(), new ColumnFamilyOptions()));
//        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
//        try(final DBOptions dboptions = new DBOptions()) {
//            db = RocksDB.open(dboptions, DB_NAME, columnFamilyDescriptors, columnFamilyHandles);
//            cfh = columnFamilyHandles.get(1);
//        }
//        catch (RocksDBException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private static void closeDB() {
//        cfh.close();
//        db.close();
//    }
//
//    @Override
//    protected RangeIndex makeRangeIndex(int order, int minRecords) {
//        RangeIndex x = new RocksRangeIndex(db, cfh, new RecordFactory(RecordLib.TestRecordLength,0));
//        x.clear();
//        return x ;
//    }
//}