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

public class TestNodeTableR {}

//public class TestNodeTableR extends AbstractTestNodeTable {
//    
//    public static void main(String...args) {
//        TestNodeTableR x = new TestNodeTableR();
//        x.createEmptyNodeTable();
//        x.afterTest();
//        System.exit(0);
//    }
//    
//    private static String testRDB = "target/RocksDB/nt/";
//    
//    private RocksTDB db;
//
//    @After
//    public void afterTest() {
//        if ( db != null )
//            db.rdb.close() ;
//    }
//    
//    @Override
//    protected NodeTable createEmptyNodeTable() {
//        FileOps.ensureDir(testRDB);
//        FileOps.clearDirectory(testRDB);
//        
//        //XXX BuildR.createNodeTable.
//        
//        BuildR.formatDatabase(testRDB);
//        db = BuildR.openDB(testRDB);
//        ColumnFamilyHandle h = BuildR.findHandle(db, BuildR.cfNodeId2Node);
//        
//        //TDBuilder.
//        // In-memory.
//        RecordFactory recordFactory = new RecordFactory(SystemTDB.LenNodeHash, SystemTDB.SizeOfNodeId) ;
//        Index nodeToId = new IndexMap(recordFactory);
//        return new RocksNodeTable(db.rdb, h, nodeToId, ByteCodecThrift.create());
//    }
//}
