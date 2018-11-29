package com.common;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/*
*@ClassName:FileUtil
 @Description:TODO
 @Author:
 @Date:2018/11/26 9:52 
 @Version:v1.0
*/
public class FileUtil {
    public static List<String> readFile(String path) throws IOException {
        /*ArrayList<String> strings = new ArrayList<String>();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
        String line;
        while ((line=bufferedReader.readLine())!=null){
            strings.add(line);
        }
        bufferedReader.close();
        return strings;*/

        return  IOUtils.readLines(new FileReader(path));
    }

    public static void writeFile(String path, String record, boolean append) throws IOException {
        PrintWriter pw = new PrintWriter(new FileWriter(path,append), true);
        pw.println(record);
        pw.close();
    }
    public static void writeFile(String path, List<String> records, boolean append) throws IOException {
        for (int i = 0; i < records.size(); i++) {
            writeFile(path,records.get(i),append);
        }
    }
}
