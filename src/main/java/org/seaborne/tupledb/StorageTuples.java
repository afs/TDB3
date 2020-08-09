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

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.storage.StorageRDF;
import org.apache.jena.dboe.transaction.txn.Transaction;
import org.apache.jena.dboe.transaction.txn.TransactionalSystem;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.DatasetChanges;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.QuadAction;
import org.apache.jena.tdb2.lib.TupleLib;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;

/** {@link StorageRDF} based on {@link NodeTupleTable}. */
public class StorageTuples implements StorageRDF {
    private NodeTupleTable      tripleTable;
    private NodeTupleTable      quadTable;
    private TransactionalSystem txnSystem;

    // In notifyAdd and notifyDelete, check whether the change is a real change or
    // not.
    // e.g. Adding a quad already present is not a real change.
    // However, that requires looking in the data so incurs a cost.
    // Normally, "false". "QuadAction.NO_*" are not used.
    private final boolean       checkForChange = false;
    private boolean             closed         = false;

    public StorageTuples(TransactionalSystem txnSystem, NodeTupleTable tripleTable, NodeTupleTable quadTable) {
        this.txnSystem = txnSystem;
        this.tripleTable = tripleTable;
        this.quadTable = quadTable;
    }

    public NodeTupleTable getQuadTable() {
        checkActive();
        return quadTable;
    }

    public NodeTupleTable getTripleTable() {
        checkActive();
        return tripleTable;
    }

    private void checkActive() {}

    // Watching changes (add, delete, deleteAny)

    private DatasetChanges monitor = null;

    public void setMonitor(DatasetChanges changes) {
        monitor = changes;
    }

    public void unsetMonitor(DatasetChanges changes) {
        if ( monitor != changes )
            throw new InternalErrorException();
        monitor = null;
    }

    private final void notifyAdd(Node g, Node s, Node p, Node o) {
        if ( monitor == null )
            return;
        QuadAction action = QuadAction.ADD;
        if ( checkForChange ) {
            if ( contains(g, s, p, o) )
                action = QuadAction.NO_ADD;
        }
        monitor.change(action, g, s, p, o);
    }

    private final void notifyDelete(Node g, Node s, Node p, Node o) {
        if ( monitor == null )
            return;
        QuadAction action = QuadAction.DELETE;
        if ( checkForChange ) {
            if ( !contains(g, s, p, o) )
                action = QuadAction.NO_DELETE;
        }
        monitor.change(action, g, s, p, o);
    }

    @Override
    public void add(Node s, Node p, Node o) {
        checkActive();
        ensureWriteTxn();
        notifyAdd(null, s, p, o);
        getTripleTable().addRow(s, p, o);
    }

    @Override
    public void add(Node g, Node s, Node p, Node o) {
        checkActive();
        ensureWriteTxn();
        notifyAdd(g, s, p, o);
        getQuadTable().addRow(g, s, p, o);
    }

    @Override
    public void delete(Node s, Node p, Node o) {
        checkActive();
        ensureWriteTxn();
        notifyDelete(null, s, p, o);
        getTripleTable().deleteRow(s, p, o);
    }

    @Override
    public void delete(Node g, Node s, Node p, Node o) {
        checkActive();
        ensureWriteTxn();
        notifyDelete(g, s, p, o);
        getQuadTable().deleteRow(g, s, p, o);
    }

    @Override
    public void removeAll(Node s, Node p, Node o) {
        checkActive();
        ensureWriteTxn();
        removeWorker(() -> getTripleTable().findAsNodeIds(s,p,o),
                     x  -> getTripleTable().getTupleTable().delete(x) );
    }

    @Override
    public void removeAll(Node g, Node s, Node p, Node o) {
        checkActive();
        ensureWriteTxn();
        removeWorker(() -> getQuadTable().findAsNodeIds(g,s,p,o),
                     x  -> getQuadTable().getTupleTable().delete(x) );
    }

    private static final int DeleteBufferSize = 1000;

    /** General purpose "remove by pattern" code */
    private void removeWorker(Supplier<Iterator<Tuple<NodeId>>> finder, Consumer<Tuple<NodeId>> deleter) {
        // Not Java11 @SuppressWarnings("unchecked")
        //Tuple<NodeId>[] buffer = (Tuple<NodeId>[])new Object[DeleteBufferSize];
        Object[] buffer = new Object[DeleteBufferSize];
        while (true) {
            Iterator<Tuple<NodeId>> iter = finder.get();
            // Get a slice
            int idx = 0;
            for (; idx < DeleteBufferSize; idx++ ) {
                if ( !iter.hasNext() )
                    break;
                buffer[idx] = iter.next();
            }
            // Delete them.
            for ( int i = 0; i < idx; i++ ) {
                @SuppressWarnings("unchecked")
                Tuple<NodeId> x = (Tuple<NodeId>)buffer[i];
                deleter.accept(x);
                buffer[i] = null;
            }
            // Finished?
            if ( idx < DeleteBufferSize )
                break;
        }
    }

    @Override
    public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
        checkActive();
        requireTxn();
        Iterator<Tuple<NodeId>> iter = getQuadTable().findAsNodeIds(g, s, p, o);
        Iterator<Quad> iter2 = TupleLib.convertToQuads(quadTable.getNodeTable(), iter);
        return iter2;
    }

    @Override
    public Iterator<Triple> find(Node s, Node p, Node o) {
        checkActive();
        requireTxn();
        Iterator<Tuple<NodeId>> iter = getTripleTable().findAsNodeIds(s, p, o);
        Iterator<Triple> iter2 = TupleLib.convertToTriples(tripleTable.getNodeTable(), iter);
        return iter2;
    }

//    @Override
//    public Stream<Quad> stream(Node g, Node s, Node p, Node o) {
//        checkActive();
//        requireTxn();
//        return Iter.asStream(find(g, s, p, o));
//    }
//
//    @Override
//    public Stream<Triple> stream(Node s, Node p, Node o) {
//        checkActive();
//        requireTxn();
//        return Iter.asStream(find(s, p, o));
//    }

    @Override
    public boolean contains(Node s, Node p, Node o) {
        checkActive();
        requireTxn();
        return getTripleTable().find(s, p, o).hasNext();
    }

    @Override
    public boolean contains(Node g, Node s, Node p, Node o) {
        checkActive();
        requireTxn();
        return getQuadTable().find(g, s, p, o).hasNext();
    }

    // This test is also done by the transactional components so no need to test here.
    private void requireTxn() {}

//    private void requireTxn() {
//        if ( ! txnSystem.isInTransaction() )
//            throw new TransactionException("Not on a transaction");
//    }

    private void ensureWriteTxn() {
        Transaction txn = txnSystem.getThreadTransaction();
        txn.ensureWriteTxn();
    }
}
