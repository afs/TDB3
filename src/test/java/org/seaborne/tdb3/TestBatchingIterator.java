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

import static org.junit.Assert.assertEquals;

import java.util.Arrays ;
import java.util.Iterator ;
import java.util.List ;
import java.util.function.BiFunction ;

import org.apache.jena.atlas.iterator.Iter ;
import org.junit.Test ;
import org.seaborne.tdb3.sys.BatchingIterator;

/** Tests for {@link BatchingIterator} */
public class TestBatchingIterator {
    
    static List<Integer> data = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9) ;
    BiFunction<Integer, Integer, Iterator<Integer>> gen = (s,f) -> {
        if ( f <= s) return Iter.nullIterator() ;
        return data.subList(fix(s), fix(f)).iterator();
    };
    
    private static int fix(int x) {
        if ( x < 0 ) return 0 ;
        if ( x > data.size() ) return data.size();
        return x ;
    }
    
    @Test public void whole_slice_1() {
        Iterator<Integer> iter = new BatchingIterator<>(0, 10, 2, gen);
        assertEquals(10, Iter.count(iter));
    }
    
    @Test public void whole_slice_2() {
        Iterator<Integer> iter = new BatchingIterator<>(0, 10, 3, gen);
        assertEquals(10, Iter.count(iter));
    }

    @Test public void whole_slice_3() {
        Iterator<Integer> iter = new BatchingIterator<>(0, 10, 10, gen);
        assertEquals(10, Iter.count(iter));
    }
    
    @Test public void whole_slice_4() {
        Iterator<Integer> iter = new BatchingIterator<>(0, 10, 100, gen);
        assertEquals(10, Iter.count(iter));
    }

    @Test public void whole_slice_5() {
        Iterator<Integer> iter = new BatchingIterator<>(-1, 10, 2, gen);
        assertEquals(10, Iter.count(iter));
    }

    @Test public void whole_slice_6() {
        Iterator<Integer> iter = new BatchingIterator<>(0, 12, 2, gen);
        assertEquals(10, Iter.count(iter));
    }
    
    @Test public void whole_slice_7() {
        Iterator<Integer> iter = new BatchingIterator<>(0, 12, 3, gen);
        assertEquals(10, Iter.count(iter));
    }

    @Test public void slice_1() {
        Iterator<Integer> iter = new BatchingIterator<>(0, 4, 2, gen);
        assertEquals(4, Iter.count(iter));
    }

    @Test public void slice_2() {
        Iterator<Integer> iter = new BatchingIterator<>(1, 4, 2, gen);
        assertEquals(3, Iter.count(iter));
    }

    @Test public void slice_3() {
        Iterator<Integer> iter = new BatchingIterator<>(1, 3, 3, gen);
        assertEquals(2, Iter.count(iter));
    }

    @Test public void slice_4() {
        Iterator<Integer> iter = new BatchingIterator<>(1, 1, 2, gen);
        assertEquals(0, Iter.count(iter));
    }
    
    @Test public void slice_5() {
        Iterator<Integer> iter = new BatchingIterator<>(2, 1, 2, gen);
        assertEquals(0, Iter.count(iter));
    }

}