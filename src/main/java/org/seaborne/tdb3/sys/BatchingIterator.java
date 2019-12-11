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
import static java.util.Objects.requireNonNull;

import java.util.Iterator ;
import java.util.List ;
import java.util.NoSuchElementException ;
import java.util.function.BiFunction ;

import org.apache.jena.atlas.iterator.Iter ;

/** An iterator that extracts batches of results from a restartable source.
 * The source has a natural ordering and can start at any point, given a start element.
 * Setting up a slice is assumed to be O(1), not a search of the source.
 * <p>
 * The source is called with a start key and it returns an iterator.
 * This iterator is used to fetch a slice, and the next key to start at.
 * Once iteration over the slice has been done, a new slice is fetched. 
 */
public class BatchingIterator<X> implements Iterator<X> {
    
    private final X start ;
    private final X finish ;
    private final BiFunction<X, X, Iterator<X>> sliceGenerator ;
    private final int sliceSize ;
    
    private X nextStart ;
    private Iterator<X> current ;
    
    private boolean slicingFinished = false ;
    private boolean finished = false ;
    // Set true if hashNext has returned true for this step.
    private boolean nextIsValid = false;

    /**
     * @param start     Beginning of iteration (inclusive)
     * @param finish    End of iteration (exclusive - depends on the {@code sliceGenerator}
     * @param sliceSize         Slice size.
     * @param  sliceGenerator A BiFunction used to get the next slicing iterator. Called with the next start and overall finish.
     */
    public BatchingIterator(X start, X finish, int sliceSize,
                            BiFunction<X, X, Iterator<X>> sliceGenerator
                            ) {
        if ( sliceSize <= 0 ) throw new IllegalArgumentException("Slice size must be positive");
        this.start = requireNonNull(start, "Argument start");
        this.nextStart = start;
        this.finish = requireNonNull(finish, "Argument finish");
        this.sliceSize = sliceSize;
        this.sliceGenerator = requireNonNull(sliceGenerator, "Argument sliceGenerator");
        this.current = null;
    }

    @Override
    public boolean hasNext() {
        if ( finished )
            return false ;
        if ( current == null ) {
            Iterator<X> iter = nextSlice();
            if ( iter == null ) {
                finished = true;
                return false;
            }
            current = iter;
        }

        while ( current != null ) {
            boolean b = current.hasNext();
            if ( b ) {
                nextIsValid = true;
                return true;
            }    
            // Close current.
            current = nextSlice();
        }
        slicingFinished = true;
        finished = true;
        return false;
    }
    
    private Iterator<X> nextSlice() {
        if ( slicingFinished )
            return null ;
        Iterator<X> iter = sliceGenerator.apply(nextStart, finish);
        List<X> elts = Iter.take(iter, sliceSize);
        if ( elts.size() < sliceSize ) {
            // Not the only way to finish.
            // Still may end exactly on a slice
            slicingFinished = true;
        } else {
            if ( iter.hasNext() ) {
                // Look one beyond the slice. 
                nextStart = iter.next();
            } else {
                slicingFinished = true;
            }
        }
        return elts.iterator();
   }

    @Override
    public X next() {
        if ( nextIsValid || hasNext() ) {
            nextIsValid = false;
            return current.next();
        }
        throw new NoSuchElementException();
    }
}