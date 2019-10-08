package net.ccic.etl.launcher;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.System.getenv;
import static net.ccic.etl.launcher.CommandBuilderUtils.checkState;
import static net.ccic.etl.launcher.CommandBuilderUtils.join;

/**
 * @Description Abstract Submit builder that defines common functionality.
 * @Author franksen
 * @CREATE 2019-08-08-下午6:03
 */
abstract class AbstractCommandBuilder {

    String mainClass;
    String sqlText;
    String runMode;
    String appName;
    String srcName;
    String primKey;
    Boolean verbose = false;
    String userName;
    String passWord;
    String url;
    String reTable;
    String beginTime;
    String endTime;
    String outputPath;
    String dateList;
    String interval;

    protected String propertiesFile;
    final List<String> appArgs;


    protected AbstractCommandBuilder() {
        this.appArgs = new ArrayList<>();
    }

    abstract List<String> buildCommand(Map<String, String> env)
        throws IOException, IllegalArgumentException;



    List<String> buildJavaCommand() throws IOException{
        List<String> cmd = new ArrayList<>();

        String[] candidateJavaHomes = new String[] {
                getenv("JAVA_HOME"),
                System.getProperty("java.home")
        };

        for (String javaHome : candidateJavaHomes) {
            if (javaHome != null) {
                cmd.add(join(File.separator, javaHome, "bin", "java"));
                break;
            }
        }


        cmd.add("-cp");
        cmd.add(join(File.pathSeparator, buildClassPath()));
        return cmd;
    }

    List<String> buildClassPath() throws IOException {
        String appHome = getAppHome();
        List<String> list = new ArrayList<>();
        list.add(join(File.separator,appHome,"conf/"));
        list.add(join(File.separator,appHome,"jars/*"));
        list.add(join(File.separator,getSparkHome(),"jars/*"));

        return list;
    }

    private String getAppHome() {
        String path = getenv("APP_HOME");
        checkState(path != null,"Don`t set the application home, So you need to set APP_HOME in System env.");
        return path;
    }


    private String getSparkHome() {
        String path = getenv("SPARK_HOME");
        checkState(path != null,"Don`t set the application home, So you need to set SPARK_HOME in System env.");
        return path;
    }


}
