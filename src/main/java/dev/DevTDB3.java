/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package dev;

import org.apache.jena.atlas.lib.DateTimeUtils ;
import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.graph.Node ;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.Quad ;
import org.apache.jena.sparql.sse.SSE ;
import org.apache.jena.system.Txn ;
import org.apache.jena.tdb2.params.StoreParams;
import org.seaborne.tdb3.DatabaseBuilderTDB3;

public class DevTDB3 {
    // Todo:

    // ToDo's
    // Prefixes.
    // Tests
    // Tidy.
    // Merge back - update TDB2.
    // Low-level filters.
    // Loader
    // Assembler

    // J4 - sort out prefixes for DatasetGraphs.

    static {
        LogCtl.setCmdLogging();
        //LogCtl.setJavaLogging();
    }

    public static void main(String[] args) {
        // TupleTable.
        String DIR = "DB3";
        FileOps.ensureDir(DIR);
        boolean cleanStart = false;

        if ( cleanStart )
            FileOps.clearAll(DIR);

        DatasetGraph dsg = DatabaseBuilderTDB3.build(Location.create(DIR), StoreParams.getDftStoreParams());

        Node g1 = SSE.parseNode(":graph1");
        Node g2 = SSE.parseNode(":graph2");
        Node s = SSE.parseNode(":s");
        Node p = SSE.parseNode(":p");
        Node n1 = SSE.parseNode("1");
        Node n2 = SSE.parseNode("2");
        Node n3 = NodeFactory.createLiteral(DateTimeUtils.nowAsXSDDateTimeString());

        if ( cleanStart ) {
            Txn.executeWrite(dsg,  ()->{
                dsg.getDefaultGraph().getPrefixMapping().setNsPrefix("ex", "http://example/");
                dsg.add(Quad.defaultGraphIRI, s, p, n1);
                dsg.add(g1, s, p, n3);
                dsg.add(g2, s, p, n3);
                dsg.add(g1, s, p, n1);
                dsg.add(g2, s, p, n1);
            });
        }
        Txn.executeRead(dsg,  ()->{
            RDFDataMgr.write(System.out, dsg, Lang.TRIG);
//            Iterator<Quad> iter = dsg.find(null, null, null, null);
//            iter.forEachRemaining(System.out::println);
            System.out.println("- - - - - - - - - - - -");
            RDFDataMgr.write(System.out, dsg.getUnionGraph(), Lang.TRIG);
//            dsg.getUnionGraph().find().forEachRemaining(System.out::println);
        });
    }
}
