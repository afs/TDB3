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

package org.seaborne.tupledb;


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
import org.apache.jena.tdb2.store.QuadTable;
import org.apache.jena.tdb2.store.StoragePrefixesTDB;
import org.apache.jena.tdb2.store.TripleTable;
import org.apache.jena.tdb2.store.nodetable.NodeTable;
import org.apache.jena.tdb2.store.nodetable.NodeTableCache;
import org.apache.jena.tdb2.store.nodetable.NodeTableInline;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTableConcrete;
import org.apache.jena.tdb2.store.tupletable.TupleIndex;
import org.apache.jena.tdb2.store.tupletable.TupleIndexRecord;
import org.apache.jena.tdb2.sys.SystemTDB;
import org.seaborne.tdb3.DatasetGraphTDB3;

/** Build a {@link DatasetGraphTuples}.
 * @implNote
 *  It is possible to call the builder to get parts, not just the whole dataset graph.
 * This is used by tests.
 *
 * Because datastructures have to tie together, a purely functional design, with
 * all arguments in the "create" calls,
 * is easier to get wrong.
 */
public abstract class AbstractBuilderDatabaseTuples {

    // Builder configuration.
    protected final Location location;
    protected final StoreParams appParams;

    // Fields for the building process.
    protected TransactionalSystem txnSystem;
    protected StoreParams params;

    protected AbstractBuilderDatabaseTuples(Location location, StoreParams appParams) {
        this.location = location;
        this.appParams = appParams;
    }

    protected abstract void error(String msg);

    public DatasetGraph buildDatasetGraph() {
        startBuild();
        initBuild(appParams);

        ReorderTransformation reorder = ReorderLib.fixed();
        this.txnSystem = createTransactionalSystem();

        StorageTuples storageRDF = createStorageRDF();
        StoragePrefixes storagePrefixes = createStoragePrefixes();

        DatasetGraph dsg = build(location, params, reorder, storageRDF, storagePrefixes, txnSystem);

        finishBuild();
        return dsg;
    }

    protected abstract DatasetGraph build(Location location2, StoreParams params2, ReorderTransformation reorder, StorageTuples storageRDF,
                                  StoragePrefixes storagePrefixes, TransactionalSystem txnSystem2);

    private void ensureInit() {

    }

    /** Setup the storage area. Decide on parameters.
     * @param appParams */
    protected void initBuild(StoreParams appParams) {
        StoreParams locParams = StoreParamsCodec.read(location);
        StoreParams dftParams = StoreParams.getDftStoreParams();
        boolean newArea = isNewDatabaseArea(location);
        if ( newArea )
            formatNewDatabaseArea(location);
        else
            fromExistingDatabaseArea(location);
        // This can write the chosen parameters if necessary (new database, appParams != null, locParams == null)
        this.params = StoreParamsFactory.decideStoreParams(location, newArea, appParams, locParams, dftParams);
    }

    protected void startBuild() { }

    protected void finishBuild() { }

    protected abstract void fromExistingDatabaseArea(Location location);

    protected abstract void formatNewDatabaseArea(Location location);

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

    public abstract TransactionalSystem createTransactionalSystem();

    public StorageTuples createStorageRDF() {
        NodeTable nodeTable = createNodeTable("nodes");
        // These are such thin clases, we bypass them.
//      TripleTable tripleTable = createTripleTable(nodeTable);
//      QuadTable quadTable = createQuadTable(nodeTable);

        TupleIndex[] tripleIndexes = createIndexes(params.getPrimaryIndexTriples(), params.getTripleIndexes());
        TupleIndex[] quadIndexes = createIndexes(params.getPrimaryIndexQuads(), params.getQuadIndexes());

        NodeTupleTable tripleNTT = new NodeTupleTableConcrete(3, tripleIndexes, nodeTable);
        NodeTupleTable quadNTT = new NodeTupleTableConcrete(4, quadIndexes, nodeTable);

        StorageTuples storageTuples = new StorageTuples(txnSystem, tripleNTT, quadNTT);
        return storageTuples;
    }

    public StoragePrefixes createStoragePrefixes() {
        NodeTable nodeTablePrefixes = createNodeTable(params.getPrefixTableBaseName());
        StoragePrefixesTDB prefixes = buildPrefixTable(nodeTablePrefixes);
        return prefixes;
    }

    protected StoragePrefixesTDB buildPrefixTable(NodeTable prefixNodes) {
        String primary = params.getPrimaryIndexPrefix();
        String[] indexes = params.getPrefixIndexes();
        TupleIndex prefixIndexes[] = createIndexes(primary, indexes);
        // No cache - the prefix mapping is a cache
        // No inline needed.
        NodeTupleTable prefixTable = new NodeTupleTableConcrete(primary.length(), prefixIndexes, prefixNodes);
        StoragePrefixesTDB x = new StoragePrefixesTDB(txnSystem, prefixTable);
        return x;
    }

    /** A triple table */
    private TripleTable createTripleTable(NodeTable nodeTable) {
        TupleIndex[] tripleIndexes = createIndexes(params.getPrimaryIndexTriples(), params.getTripleIndexes());
        return new TripleTable(tripleIndexes, nodeTable);
    }

    /** A quad table */
    private QuadTable createQuadTable(NodeTable nodeTable) {
        TupleIndex[] quadIndexes = createIndexes(params.getPrimaryIndexQuads(), params.getQuadIndexes());
        return new QuadTable(quadIndexes, nodeTable);
    }

    public TupleIndex[] createIndexes(String primary, String[] indexNames) {
        int indexRecordLen = primary.length()*SystemTDB.SizeOfNodeId;
        TupleIndex indexes[] = new TupleIndex[indexNames.length];
        for (int i = 0; i < indexes.length; i++) {
            String indexName = indexNames[i];
            String indexLabel = indexNames[i];
            indexes[i] = createTupleIndex(primary, indexName, indexLabel);
        }
        return indexes;
    }

    public TupleIndex createTupleIndex(String primary, String index, String indexLabel) {
        TupleMap cmap = TupleMap.create(primary, index);
        RecordFactory rf = new RecordFactory(SystemTDB.SizeOfNodeId * cmap.length(), 0);
        RangeIndex rIdx = createRangeIndex(rf, index);
        TupleIndex tIdx = new TupleIndexRecord(primary.length(), cmap, index, rf, rIdx);
        return tIdx;
    }

    /**
     *  Create a {@link NodeTable}, with storage, inlining and node cache.
     */
    public NodeTable createNodeTable(String name) {
        NodeTable nodeTable = createBaseNodeTable(name);
        nodeTable = NodeTableCache.create(nodeTable, params);
        nodeTable = NodeTableInline.create(nodeTable);
        return nodeTable;
    }

    /**
     * Create an {@link Index}.
     */
    public Index createIndex(RecordFactory rf, String index) {
        return createRangeIndex(rf, index);
    }

    /**
     * Create an {@link RangeIndex}.
     */
    public abstract RangeIndex createRangeIndex(RecordFactory rf, String index);

    /**
     * Create a {@link NodeTable} for storage - no inlining, no cache.
     * @see #createNodeTable
     */
    public abstract NodeTable createBaseNodeTable(String name);
}
