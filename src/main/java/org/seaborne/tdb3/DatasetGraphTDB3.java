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
import org.apache.jena.dboe.storage.StoragePrefixes;
import org.apache.jena.dboe.transaction.txn.TransactionalSystem;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.tdb2.params.StoreParams;
import org.rocksdb.RocksDBException;
import org.seaborne.tdb3.sys.RocksTDB;
import org.seaborne.tupledb.DatasetGraphTuples;
import org.seaborne.tupledb.StorageTuples;

public class DatasetGraphTDB3 extends DatasetGraphTuples {

    private final RocksTDB rtdb;

    public DatasetGraphTDB3(RocksTDB rtdb, Location location, StoreParams params, ReorderTransformation reorderTransformation, StorageTuples storage,
                            StoragePrefixes prefixes, TransactionalSystem txnSystem) {
        super(location, params, reorderTransformation, storage, prefixes, txnSystem);
        this.rtdb = rtdb;
    }

    public void compact() {
        try {
            rtdb.rdb.compactRange();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }



}