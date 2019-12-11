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

import java.util.List;

import org.apache.jena.dboe.base.file.Location;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.TransactionDB;

/** Info for an open database */
public class RocksTDB {
    public final TransactionDB rdb;
    //public final OptimisticTransactionDB rdb;
    public final Location location ;
    public final List<ColumnFamilyDescriptor> descriptors;
    public final List<ColumnFamilyHandle> cfHandles;

    public RocksTDB(TransactionDB rdb, Location location, List<ColumnFamilyDescriptor> descriptors, List<ColumnFamilyHandle> cfHandles) {
        this.rdb = rdb ;
        this.location = location;
        this.descriptors = descriptors ;
        this.cfHandles = cfHandles ;
    }
}