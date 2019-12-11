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

import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.dboe.base.record.RecordFactory;
import org.apache.jena.dboe.index.Index;
import org.apache.jena.dboe.index.RangeIndex;
import org.apache.jena.dboe.transaction.txn.*;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.tdb2.params.StoreParams;
import org.apache.jena.tdb2.store.nodetable.NodeTable;
import org.apache.jena.tdb2.sys.SystemTDB;
import org.rocksdb.ColumnFamilyHandle;
import org.seaborne.tdb3.rdata.ByteCodecThrift;
import org.seaborne.tdb3.rdata.RocksNodeTable;
import org.seaborne.tdb3.rdata.RocksRangeIndex;
import org.seaborne.tdb3.sys.BuildR;
import org.seaborne.tdb3.sys.RocksTDB;
import org.seaborne.tdb3.txnr.TransactionalComponentR;
import org.slf4j.Logger;

public class DatabaseBuilderTDB3 extends AbstractDatabaseBuilder {

    private RocksTDB rtdb;

    public static DatasetGraph build(Location location, StoreParams appParams) {
        DatabaseBuilderTDB3 builder = new DatabaseBuilderTDB3();
        return builder.buildDatasetGraph(location, appParams);
    }

    // XXX Engine-ise.
    private DatabaseBuilderTDB3() {
        super();
        // XXX
    }

    @Override
    protected void fromExistingDatabaseArea(Location location) {
        rtdb = BuildR.openDB(location.getDirectoryPath());
    }

    @Override
    public TransactionalSystem createTransactionalSystem() {
        TransactionalComponent tComp = new TransactionalComponentR(rtdb);
        Transaction txn = null;
        // No journal needed.
        TransactionCoordinator tc = new TransactionCoordinator(Location.mem());
        tc.add(tComp);
        tc.start();
        return new TransactionalBase(location.getDirectoryPath(), tc);
    }

    @Override
    protected RangeIndex createRangeIndex(RecordFactory recordFactory, String nIndex) {
        ColumnFamilyHandle handle = BuildR.findHandle(rtdb, nIndex);
        RangeIndex rIdx = new RocksRangeIndex(rtdb.rdb, handle, recordFactory);
        return rIdx;
    }

    @Override
    protected NodeTable createBaseNodeTable(String name) {
        String nDat = name+":NodeHash2Id";
        String nIndex = name+":NodeId2Node";

        ColumnFamilyHandle h = BuildR.findHandle(rtdb, nDat);
        RecordFactory recordFactory = new RecordFactory(SystemTDB.LenNodeHash, SystemTDB.SizeOfNodeId) ;
        Index hashNodeIndex = createRangeIndex(recordFactory, nIndex);
        NodeTable nt = new RocksNodeTable(rtdb.rdb, h, hashNodeIndex, ByteCodecThrift.create());
        return nt;
    }

    @Override
    protected void formatNewDatabaseArea(Location location) {
        BuildR.formatDatabase(location.getDirectoryPath());
        fromExistingDatabaseArea(location);
    }

    private static Logger LOG = org.slf4j.LoggerFactory.getLogger(DatabaseBuilderTDB3.class);

    @Override
    protected void error(String msg) {
        if ( LOG != null )
            LOG.error(msg);
        throw new TDB3Exception(msg);
    }

}
