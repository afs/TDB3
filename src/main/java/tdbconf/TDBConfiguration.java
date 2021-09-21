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
import java.io.FileInputStream ;
import java.io.IOException ;
import java.io.Reader ;
import java.util.* ;
import java.util.regex.Matcher ;
import java.util.regex.Pattern ;

import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.lib.Pair ;
import org.apache.jena.atlas.logging.Log ;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.dboe.sys.Names;
import org.apache.jena.tdb2.params.StoreParams;
import org.apache.jena.util.FileUtils ;

// SOME OLD CODE

public class TDBConfiguration
{
    private List<String> indexes3 ;
    private List<String> indexes4 ;
    private StoreParams systemParams ;


    // Params.

    private TDBConfiguration() {}

    public static TDBConfiguration create(Location location)
    {
        TDBConfiguration locDesc = new TDBConfiguration() ;

        Pair<List<String>,List<String>> idxs = TDBConfiguration.indexes(location) ;
        locDesc.indexes3 = idxs.getLeft() ;
        locDesc.indexes4 = idxs.getRight() ;

        locDesc.systemParams = StoreParams.getDftStoreParams() ;

        String conf = location.getPath(Names.datasetConfig) ;
        if ( FileOps.exists(conf) )
        {
            try {

            Reader r = FileUtils.asBufferedUTF8(new FileInputStream(conf)) ;
            Properties p = new Properties() ;
            p.load(r) ;
            // Process.
            } catch (IOException ex)
            {
                Log.warn(TDBConfiguration.class, "Failed to read the configuration file: "+ex.getMessage()) ;
            }
        }

        return locDesc ;
    }

    public List<String> getIndexesTriples()
    {
        return Collections.unmodifiableList(indexes3) ;
    }

    public List<String> getIndexesQuads()
    {
        return Collections.unmodifiableList(indexes4) ;
    }

    @Override
    public String toString()
    {
        return indexes3+" "+indexes4 ;
    }

    // Not Java's finest hour ...
    public static Pair<List<String>, List<String>> indexes(Location location)
    {
        File f = new File(location.getDirectoryPath()) ;
        String[] files = f.list() ;
        if ( files == null )
            return null ;
        return recombobulate(Arrays.asList(files)) ;
//        // Java 7.
//        Path path = Paths.get(location.getDirectoryPath()) ;
//        try {
//            List<String> entries = new ArrayList<>() ;
//            try(DirectoryStream<Path> dStream = Files.newDirectoryStream(path))
//            {
//                for( Path entry : dStream)
//                {
//                    String name = entry.getFileName().toString() ;
//                    entries.add(name) ;
//                }
//            }
//            return recombobulate(entries) ;
//        } catch (NotDirectoryException ex)
//        {
//            return null ;
//        }
//        catch (IOException ex) { IO.exception(ex) ; return null ; }
    }

    // Not Java's finest hour ...

    private static Pattern patternIDN = Pattern.compile("^(.*)\\.idn$") ;
    private static Comparator<String> comparator = new Comparator<String>() {
    @Override
    public int compare(String s1, String s2)
    {
        // Strings of equal length with G>S>P>O
        return Integer.compare(weight(s1),weight(s2)) ;

    }

    private int weight(String s)
    {
        int w = 0 ;
        for ( int i = 0 ; i < s.length() ; i++ )
        {
            char ch = s.charAt(i) ;
            int x ;
            switch (ch)
            { case 'G': x = 1 ; break ;
              case 'S': x = 2 ; break ;
              case 'P': x = 3 ; break ;
              case 'O': x = 4 ; break ;
              default: x = 5 ;
            }
            w = (w<<3)+x ;
        }
        return w ;
    }} ;

    private static Pair<List<String>, List<String>> recombobulate(List<String> entries)
    {
        List<String> indexes3 = new ArrayList<>() ;
        List<String> indexes4 = new ArrayList<>() ;
        for( String entry : entries)
        {
            // Unsorted.
            Matcher m = patternIDN.matcher(entry) ;
            if ( m.matches() )
            {
                String idxName = m.group(1) ;
                if ( checkIndexName(idxName) )
                    ((idxName.length()==3)?indexes3:indexes4).add(idxName) ;
            }
        }
        Collections.sort(indexes3, comparator) ;
        Collections.sort(indexes4, comparator) ;
        return Pair.create(indexes3, indexes4) ;
    }

    private static boolean checkIndexName(String idxName)
    {
//        if ( idxName.length() != 3 && idxName.length() != 4 )
//            return null ;

        for ( int i = 0 ; i < idxName.length() ; i++ )
        {
            char ch = idxName.charAt(i) ;
            switch (ch)
            { case 'G': case 'S': case 'P': case 'O': break ;
              default: return false ;
            }
        }
        return true ;
    }


}

