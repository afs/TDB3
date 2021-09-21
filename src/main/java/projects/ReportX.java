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

package projects;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.atlas.lib.DateTimeUtils;

public class ReportX {

    public static void main0(String[] args) {
        String r = "^([a-z|A-Z|0-9|%\\(\\)-\\+\\[\\]\\|:',\\./]+[ ]*)+$";
//      String r = "^([a-z|A-Z|0-9|%\\(\\)-\\+\\[\\]\\|:',\\./]+[ ]+)*[a-z|A-Z|0-9|%\\(\\)-\\+\\[\\]\\|:',\\./]+$";
        String x = "a{";
        while (true) {
            System.out.println("Text = " + x);
            System.out.printf("Len = %d\n", x.length());
            long start = System.currentTimeMillis();
            Pattern p = Pattern.compile(r);
            Matcher m = p.matcher(x);
            m.find();
            long end = System.currentTimeMillis();
            System.out.printf("Time %.3fs\n\n", 0.001 * (end - start));
            x = "a" + x;
        }
    }

    public static void main(String[] args) throws Exception {
        String x = "abcdef ";
        for(int i = 0 ; i < 5; i++ ) {
            x += x;
        }
        System.out.printf("Len = %d\n", x.length());

        System.out.println(DateTimeUtils.nowAsXSDDateTimeString());
        for(int i = 0 ; i < 10; i++ ) {
            String r = "^([a-z|A-Z|0-9|%\\(\\)-\\+\\[\\]\\|:',\\./]+[ ]*)+$";
//                String r = "^[a-z|A-Z|0-9|%\\(\\)-\\+\\[\\]\\|:',\\./]+([ ]+[a-z|A-Z|0-9|%\\(\\)-\\+\\[\\]\\|:',\\./]+)*$";
//                String r = "^([a-z]+[ ]*)+$";

            Pattern p = Pattern.compile(r);
            Matcher m = p.matcher(x+"£");
            boolean b = m.find();
            if ( ! b )
                break;
            x = x+"X";
        }
        System.out.println(DateTimeUtils.nowAsXSDDateTimeString());
        System.exit(0);
    }
}