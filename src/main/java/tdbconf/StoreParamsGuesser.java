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

import java.io.File ;
import java.io.FileFilter ;
import java.nio.file.Path ;
import java.nio.file.Paths ;
import java.util.ArrayList;
import java.util.Arrays ;
import java.util.List ;

import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.dboe.sys.Names;
import org.apache.jena.ext.com.google.common.collect.ArrayListMultimap ;
import org.apache.jena.ext.com.google.common.collect.ListMultimap ;
import org.apache.jena.tdb2.params.StoreParams;

/** Build a TDB system parameters object based on a location */
public class StoreParamsGuesser {

    private static final String ConfigFileName = "tdb.conf" ;

    private static List<String> indexesExts = Arrays.asList(Names.extBptTree, Names.extBptRecords, Names.extBptState);

    //private static List<String> directExts = Arrays.asList(Names.extBPT) ;

    public static StoreParams configure(Location loc) {
        StoreParams params = null ;
        if ( loc.isMem() ) {
            // Can't
            throw new IllegalArgumentException("Can only build parameters from a disk location") ;
        }

        String dir = loc.getDirectoryPath() ;
        Path path = Paths.get(dir) ;

        if ( loc.exists(ConfigFileName) ) {
            }

        // All the non-hidden files, with a dot in their name.
        FileFilter fnf = new FileFilter() {
            @Override
            public boolean accept(File file) {
                String fn = file.getName() ;
                if ( fn.startsWith(".") )
                    return false ;
                if ( ! fn.contains(".") )
                    return false ;
                if ( file.isDirectory() )
                    return false ;
                return true ;
            }
        };

        // Break up file names.
        List<File> files = Arrays.asList(path.toFile().listFiles(fnf)) ;

        // Name to extensions found.
        final ListMultimap<String, String> fileParts = ArrayListMultimap.create() ;
        // Map base name to extensions found
        files.stream().forEachOrdered((item)->{
            String name = item.getName() ;
            String base = FileOps.basename(name) ;
            String ext = FileOps.extension(name) ;
            fileParts.put(base, ext) ;
        }) ;

        List<String> indexesTriples = new ArrayList<>();
        List<String> indexesQuads = new ArrayList<>();

        // Index seeking.
        for ( String base : fileParts.keys() ) {
            List<String> exts = fileParts.get(base) ;

            if ( base.length() == 3 && tripleIndex(base, exts) ) {
                indexesTriples.add(base) ;
                continue ;
            }
            if ( base.length() == 4 && quadIndex(base, exts) ) {
                indexesQuads.add(base) ;
                continue ;
            }
        }

        String[] dummy = {} ;
        String tripleIndexes[] = indexesTriples.toArray(dummy) ;
        String quadIndexes[] = indexesQuads.toArray(dummy) ;

        return params ;
    }


    private static boolean tripleIndex(String base, List<String> extensions) {
        if ( extensions == null ) return false ;
        //if ( base.length() != 3 ) return false ;
        if ( ! base.matches("^[SPO]{3}$") ) return false ;
        return checkParts(base, extensions, indexesExts) ;
    }

    private static boolean quadIndex(String base, List<String> extensions) {
        if ( extensions == null ) return false ;
        //if ( base.length() != 4 ) return false ;
        if ( ! base.matches("^[GSPO]{4}$") ) return false ;
        return checkParts(base, extensions, indexesExts) ;
    }

    private static boolean checkParts(String base, List<String> extensions, List<String> expected) {
        if ( extensions.size() != expected.size()) {
            System.out.printf("Warning: bad index: %s extensions=%s expected=%s\n", base, extensions, expected) ;
            return false;
        }

        // Assumes extensions are not repeated.
        for ( String ext : extensions ) {
            if ( expected.contains(ext) ) continue ;
            System.out.printf("Warning: bad index: %s extensions=%s expected=%s\n", base, extensions, expected) ;
            return false ;
        }
        return true ;
    }


}
