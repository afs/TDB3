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


import java.io.File;
import java.io.FileFilter;

import org.apache.jena.atlas.lib.tuple.TupleMap;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.dboe.base.record.RecordFactory;
import org.apache.jena.dboe.index.Index;
import org.apache.jena.dboe.index.RangeIndex;
import org.apache.jena.dboe.storage.StoragePrefixes;
import org.apache.jena.dboe.sys.Names;
import org.apache.jena.dboe.transaction.txn.TransactionalSystem;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderLib;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.tdb2.params.StoreParams;
import org.apache.jena.tdb2.params.StoreParamsCodec;
import org.apache.jena.tdb2.params.StoreParamsFactory;
import org.apache.jena.tdb2.store.StoragePrefixesTDB;
import org.apache.jena.tdb2.store.nodetable.NodeTable;
import org.apache.jena.tdb2.store.nodetable.NodeTableInline;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTableConcrete;
import org.apache.jena.tdb2.store.tupletable.TupleIndex;
import org.apache.jena.tdb2.store.tupletable.TupleIndexRecord;
import org.apache.jena.tdb2.sys.SystemTDB;

public abstract class AbstractDatabaseBuilder {

    protected Location location;
    protected StoreParams params;
    protected TransactionalSystem txnSystem;

//    protected AbstractDatabaseBuilder(Location location, StoreParams params) {
//    }

    protected AbstractDatabaseBuilder() {}

    protected DatasetGraph buildDatasetGraph(Location location, StoreParams appParams) {
        this.location = location;
        StoreParams locParams = StoreParamsCodec.read(location);
        StoreParams dftParams = StoreParams.getDftStoreParams();
        boolean newArea = isNewDatabaseArea(location);
        if ( newArea )
            formatNewDatabaseArea(location);
        else
            fromExistingDatabaseArea(location);
        // This can write the chosen parameters if necessary (new database, appParams != null, locParams == null)
        this.params = StoreParamsFactory.decideStoreParams(location, newArea, appParams, locParams, dftParams);

        ReorderTransformation reorder = ReorderLib.fixed();
        this.txnSystem = createTransactionalSystem();

        StorageTuples storageRDF = createStorageRDF(txnSystem);
        StoragePrefixes storagePrefixes = createStoragePrefixes();
        DatasetGraph dsg = new DatasetGraphAny(location, params, reorder,
            storageRDF, storagePrefixes, txnSystem);
        return dsg;
    }

    protected abstract void fromExistingDatabaseArea(Location location);

    private static boolean isNewDatabaseArea(Location location) {
        if ( location.isMem() )
            return true;
        File d = new File(location.getDirectoryPath());
        if ( !d.exists() )
            return true;
        FileFilter ff = fileFilterNewDB;
        File[] entries = d.listFiles(ff);
        return entries.length == 0;
    }

    /**
     * FileFilter
     * Skips "..", "." "tdb.lock", and "tdb.cfg"
     */
    private static  FileFilter fileFilterNewDB  = (pathname)->{
        String fn = pathname.getName();
        if ( fn.equals(".") || fn.equals("..") )
            return false;
        if ( pathname.isDirectory() )
            return true;
        if ( fn.equals(Names.TDB_CONFIG_FILE) )
            return false;
        if ( fn.equals(Names.TDB_LOCK_FILE) )
            return false;
        return true;
    };

    protected abstract void error(String msg);

    protected abstract void formatNewDatabaseArea(Location location);

    // ---- High level.

    public StorageTuples createStorageRDF(TransactionalSystem txnSystem) {
        NodeTable nodeTable = createNodeTable("nodes");

        TupleIndex[] tripleIndexes = createIndexes(params.getPrimaryIndexTriples(), params.getTripleIndexes());
        TupleIndex[] quadIndexes = createIndexes(params.getPrimaryIndexQuads(), params.getQuadIndexes());

        NodeTupleTable tripleTable = new NodeTupleTableConcrete(3,tripleIndexes, nodeTable);
        NodeTupleTable quadTable = new NodeTupleTableConcrete(4, quadIndexes, nodeTable);

        StorageTuples storageTuples = new StorageTuples(txnSystem, tripleTable, quadTable);
        return storageTuples;
    }

    public StoragePrefixes createStoragePrefixes() {
        NodeTable nodeTablePrefixes = createNodeTable(params.getPrefixTableBaseName());
        StoragePrefixesTDB prefixes = buildPrefixTable(nodeTablePrefixes);
        return prefixes;
    }

    private StoragePrefixesTDB buildPrefixTable(NodeTable prefixNodes) {
        String primary = params.getPrimaryIndexPrefix();
        String[] indexes = params.getPrefixIndexes();

        TupleIndex prefixIndexes[] = createIndexes(primary, indexes);
        // XXX
//        if ( prefixIndexes.length != 1 )
//            error(log, "Wrong number of triple table tuples indexes: "+prefixIndexes.length);

        // No cache - the prefix mapping is a cache
        //NodeTable prefixNodes = makeNodeTable(location, pnNode2Id, pnId2Node, -1, -1, -1);
        NodeTupleTable prefixTable = new NodeTupleTableConcrete(primary.length(),
                                                                prefixIndexes,
                                                                prefixNodes);
        StoragePrefixesTDB x = new StoragePrefixesTDB(txnSystem, prefixTable);
        //DatasetPrefixesTDB prefixes = new DatasetPrefixesTDB(prefixTable);
        //log.debug("Prefixes: "+primary+" :: "+String.join(",", indexes));
        return x;
    }


    // Abstract
    public abstract TransactionalSystem createTransactionalSystem();

//    public TripleTable createTripleTable() { return null; }
//
//    public QuadTable createQuadTable() { return null; }

    protected TupleIndex[] createIndexes(String primary, String[] indexNames) {
        int indexRecordLen = primary.length()*SystemTDB.SizeOfNodeId;
        TupleIndex indexes[] = new TupleIndex[indexNames.length];
        for (int i = 0; i < indexes.length; i++) {
            String indexName = indexNames[i];
            String indexLabel = indexNames[i];
            indexes[i] = createTupleIndex(primary, indexName, indexLabel);
        }
        return indexes;
    }


    protected TupleIndex createTupleIndex(String primary, String index, String indexLabel) {
        TupleMap cmap = TupleMap.create(primary, index);
        RecordFactory rf = new RecordFactory(SystemTDB.SizeOfNodeId * cmap.length(), 0);
        RangeIndex rIdx = createRangeIndex(rf, index);
        TupleIndex tIdx = new TupleIndexRecord(primary.length(), cmap, index, rf, rIdx);
        return tIdx;
    }

    public NodeTable createNodeTable(String name) {
        NodeTable nodeTable = createBaseNodeTable(name);
        // Needed? nodeTable = NodeTableCache.create(nodeTable, params);
        nodeTable = NodeTableInline.create(nodeTable);
        return nodeTable;
    }

    // -- Basics
    //public TupleIndex createTupleIndex(String name, String indexName, String indexLabel) {

    protected abstract RangeIndex createRangeIndex(RecordFactory rf, String index);
    protected /*abstract*/ Index createIndex(RecordFactory rf, String index) { return createRangeIndex(rf, index); }
    protected abstract NodeTable createBaseNodeTable(String name);
}
