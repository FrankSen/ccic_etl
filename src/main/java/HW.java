import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Create By: franksen 2019-05-28 23:23
 */
public class HW {


    public static void main(String[] args) {
       /* String path = "/home/franksen/MyIdeaProject/ccic_etl/src/main/resources/file.uat";
        byte[] b = new byte[1024];
        int bs = 0;
        try {
            FileInputStream fis = new FileInputStream(file);
            while( (bs = fis.read()) !=1){
                String str = new String(b, 0, bs);
                System.out.print(str);
//                System.out.println((char) bs);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        System.out.println(file_name);*/

       File fileRead = new File("/home/franksen/MyIdeaProject/ccic_etl/src/main/resources/file.uat");
       File fileWrite = new File("filewrite.uat");

        FileReader fr = null;
        BufferedReader br = null;

        FileWriter fw = null;
        BufferedWriter bw = null;
        String line = null;
        Map<String, String> map = new HashMap<String, String>();
        try {
             fr = new FileReader(fileRead);
             br = new BufferedReader(fr);

             fw = new FileWriter(fileWrite);
             bw = new BufferedWriter(fw);

             while ((line = br.readLine()) != null){

//                 System.out.println(line);
                 String[] value = line.split(",");
                 for (int i = 0; i< value.length; i++) {
                     map.put(value[1],value[0]);
                 }
             }

             System.out.println(map.size());
            for (String s : map.values()) {

                System.out.println(s);

            }
            br.close();
            bw.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
