package day03;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.TreeMap;

public class demo01 {
    public static void main(String[] args) throws IOException {
        URL uri = demo01.class.getClassLoader().getResource("a.txt");
        String path = uri.getPath();
        //System.out.println(path);
        String s = FileUtils.readFileToString(new File(path), "utf-8");
        //System.out.println(s);
        Map map = new TreeMap();
        String[] sp = s.split("\\s+");
        for (String s1 : sp) {
            //System.out.println(s1);
            if (map.containsKey(s1)) {
                Integer value = (Integer) map.get(s1);
                map.put(s1,value+1  );
            }else {
                map.put(s1,1);
            }
        }
        System.out.println(map);
    }
}
