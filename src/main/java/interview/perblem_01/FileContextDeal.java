package interview.perblem_01;

import java.io.*;

/**
 * Create By: franksen 2019-05-29 21:22
 */
public class FileContextDeal {


    private FileReader fr;
    private BufferedReader br;

    private FileWriter fw = null;
    private BufferedWriter bw = null;

    public static void main(String[] args){
        FileContextDeal fcd = new FileContextDeal();
        fcd.readFile("/home/franksen/MyIdeaProject/ccic_etl/src/resources/file_new.uat"
                , "/home/franksen/MyIdeaProject/ccic_etl/src/resources/file_name_list.txt");


    }

    private void readFile(String filepath, String outputpath){
        File file = new File(filepath);

        String line = null;
        int tmp = 0;
        StringBuffer sb = null;
        try{
            fr = new FileReader(file);
            br = new BufferedReader(fr);

            fw = new FileWriter(outputpath);
            bw = new BufferedWriter(fw);
            sb = new StringBuffer();

            while((line = br.readLine()) != null){
                String fileUrl = line.split(",")[2];
                String[] fileArr = fileUrl.split("/");
                String fileName = fileArr[fileArr.length-1];


                sb.append(fileName.replaceAll("vs_test","vs_qa").replaceAll(" ","_") + "\n");

                tmp ++;
                if(tmp == 5){
                    bw.write(sb.toString());
                    System.out.println("write table name num is: " + tmp);
                    tmp = 0;
                }

            }

            br.close();
            bw.close();


        } catch (FileNotFoundException e){
            e.printStackTrace();
        } catch(IOException e){
            e.printStackTrace();
        }

    }

}
