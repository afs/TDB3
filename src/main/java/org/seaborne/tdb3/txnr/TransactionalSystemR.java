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

package org.seaborne.tdb3.txnr;

import org.apache.jena.dboe.transaction.txn.*;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;

public class TransactionalSystemR implements TransactionalSystem {

    @Override
    public void begin(TxnType type) {}

    @Override
    public void begin(ReadWrite readWrite) {}

    @Override
    public boolean promote(Promote mode) {
        return false;
    }

    @Override
    public void abort() {}

    @Override
    public void end() {}

    @Override
    public ReadWrite transactionMode() {
        return null;
    }

    @Override
    public TxnType transactionType() {
        return null;
    }

    @Override
    public boolean isInTransaction() {
        return false;
    }

    @Override
    public void commitPrepare() {}

    @Override
    public void commitExec() {}

    @Override
    public TransactionCoordinatorState detach() {
        return null;
    }

    @Override
    public void attach(TransactionCoordinatorState coordinatorState) {}

    @Override
    public TransactionCoordinator getTxnMgr() {
        return null;
    }

    @Override
    public TransactionInfo getTransactionInfo() {
        return null;
    }

    @Override
    public Transaction getThreadTransaction() {
        return null;
    }

}
