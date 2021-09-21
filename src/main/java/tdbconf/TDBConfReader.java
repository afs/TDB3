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

package tdbconf;

public class TDBConfReader {}

//import java.io.File ;
//
//import org.apache.jena.query.ARQ ;
//import org.apache.jena.sparql.engine.ExecutionContext ;
//import org.apache.jena.sparql.expr.Expr ;
//import org.apache.jena.sparql.expr.NodeValue ;
//import org.apache.jena.sparql.function.FunctionEnv ;
//import org.apache.jena.sparql.util.ExprUtils ;
//
//public class TDBConfReader {
//    public static void main(String... argv) throws Exception {
//        Config conf = ConfigFactory.parseFile(new File("tdb.conf")) ;
//        long z = conf.getBytes("tdb.store.blocksize") ;
//        String x = conf.getString("tdb.segment_size") ;
//        Expr expr = ExprUtils.parse(x, null) ;
//        FunctionEnv env = new ExecutionContext(ARQ.getContext(), null, null, null) ;
//        NodeValue r = expr.eval(null, env) ;
//        long val = r.getInteger().longValue() ;
//        //        Node n = r.asNode() ;
////        String s = NodeFmtLib.displayStr(n) ;
////        }
//    }
//}
