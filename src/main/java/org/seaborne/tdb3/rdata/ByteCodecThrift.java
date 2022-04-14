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

package org.seaborne.tdb3.rdata;

import org.apache.jena.graph.Node ;
import org.apache.jena.riot.thrift.ThriftConvert ;
import org.apache.jena.riot.thrift.wire.RDF_Term ;
import org.apache.jena.tdb2.TDBException ;
import org.apache.thrift.TDeserializer ;
import org.apache.thrift.TSerializer ;
import org.apache.thrift.protocol.TCompactProtocol ;
import org.apache.thrift.transport.TTransportException;
import org.seaborne.tdb3.ByteCodec;

/**
 * Codec based on Thrift. This codec is not thread-safe
 */
public class ByteCodecThrift implements ByteCodec<Node> {
    public static ByteCodecThrift create() { return new ByteCodecThrift(); }

    // TSerializer and TDeserializer are single-threaded.
    // We can preallocate space.
    private final RDF_Term t = new RDF_Term();
    private final TSerializer serializer;
    private final TDeserializer deserializer;

    public ByteCodecThrift() {
        try {
            serializer = new TSerializer(new TCompactProtocol.Factory());
            deserializer = new TDeserializer(new TCompactProtocol.Factory());
        } catch (TTransportException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] encode(Node x) {
        if ( x == null ) return null;
        try {
            t.clear();
            ThriftConvert.toThrift(x, t);
            return serializer.serialize(t);
        } catch (Exception e) { throw new TDBException(e); }
    }

    @Override
    public Node decode(byte[] b) {
        if ( b == null ) return null;
        try {
            t.clear();
            deserializer.deserialize(t, b);
            return ThriftConvert.convert(t);
        } catch (Exception e) { throw new TDBException(e); }
    }
}
