package net.ccic.etl.launcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static net.ccic.etl.launcher.CommandBuilderUtils.checkArgument;

/**
 * @Description
 * @Author franksen
 * @CREATE 2019-08-09-下午5:09
 */
public class EtlSubmitCommandBuilder extends AbstractCommandBuilder {

    static final String SPARK_PARAMETER_FILE_NAME = "";
    static final String SPARK_PARAMETER_FILE_NAMES = "";

    private final boolean isExample;
    private final List<String> parsedArgs;
    private  String className;

    EtlSubmitCommandBuilder(){
        this.isExample = false;
        this.parsedArgs = new ArrayList<>();
    }

    EtlSubmitCommandBuilder(List<String> args) {
        List<String> submitArgs = args;
        boolean isExample = false;
        this.parsedArgs = new ArrayList<>();

        if(args.size() > 0){

            this.isExample = isExample;
            OptionParser parser = new OptionParser(true);
            parser.parse(submitArgs);

        }else {
            this.isExample = isExample;
        }

    }

    EtlSubmitCommandBuilder(List<String> args, String classes) {
        List<String> submitArgs = args;
        boolean isExample = false;
        this.parsedArgs = new ArrayList<>();
        this.className = classes;

        if(args.size() > 0){

            this.isExample = isExample;
            OptionParser parser = new OptionParser(true);
            parser.parse(submitArgs);

        }else {
            this.isExample = isExample;
        }

    }

    @Override
    List<String> buildCommand(Map<String, String> env) throws IOException, IllegalArgumentException {
        return buildEtlSubmitCommand(env);
    }

    private List<String> buildEtlSubmitCommand(Map<String, String> env)
            throws IOException, IllegalArgumentException{

        List<String> cmd = buildJavaCommand();
        cmd.add("-Xmx" + "1g");

        if(className != null){

            cmd.add(className);
        }else{
            cmd.add("net.ccic.etl.submit.EtlSubmit");
        }


        cmd.addAll(buildEtlSubmitArgs());
        return cmd;
    }

    List<String> buildEtlSubmitArgs(){

        List<String> args = new ArrayList<>();
        OptionParser parser = new OptionParser(false);

        if (mainClass != null){
            args.add(parser.CLASS);
            args.add(mainClass);
        }

        if(sqlText != null){
            args.add(parser.SQLTEXT);
            args.add(sqlText);
        }

        if(appName != null){
            args.add(parser.APPNAME);
            args.add(appName);
        }

        if(runMode != null){
            args.add(parser.RUN_MODE);
            args.add(runMode);
        }

        if(primKey != null){
            args.add(parser.PRIMKEY);
            args.add(primKey);
        }

        if(srcName != null){
            args.add(parser.SRCNAME);
            args.add(srcName);
        }

        if(userName != null){
            args.add(parser.USERNAME);
            args.add(userName);
        }

        if(passWord != null){
            args.add(parser.PASSWORD);
            args.add(passWord);
        }

        if(url != null){
            args.add(parser.URL);
            args.add(url);
        }

        if(reTable != null){
            args.add(parser.RETABLE);
            args.add(reTable);
        }

        if(beginTime != null){
            args.add(parser.BEGINTIME);
            args.add(beginTime);
        }

        if (endTime != null){
            args.add(parser.ENDTIME);
            args.add(endTime);
        }

        if (outputPath != null){
            args.add(parser.OUTPUTFILE);
            args.add(outputPath);
        }

        if (dateList != null){
            args.add(parser.DATELIST);
            args.add(dateList);
        }

        if (interval != null){
            args.add(parser.INTERVAL);
            args.add(interval);
        }

        if (verbose){
            args.add(parser.VERBOSE);
        }


        args.addAll(parsedArgs);


        return args;
    }

    private class OptionParser extends EtlSubmitOptionParser{
        private final boolean errorOnUnknownArgs;

        private OptionParser(boolean errorOnUnknownArgs) {
            this.errorOnUnknownArgs = errorOnUnknownArgs;
        }


        @Override
        protected boolean handle(String opt, String value) {
            switch (opt){

                case CLASS:
                    mainClass = value;
                    break;

                case SQLTEXT:
                    sqlText = value;
                    break;

                case APPNAME:
                    appName = value;
                    break;

                case RUN_MODE:
                    runMode = value;
                    break;

                case PRIMKEY:
                    primKey = value;
                    break;

                case VERBOSE:
                case VB:
                    verbose = true;
                    break;

                case USERNAME:
                case USER:
                    userName = value;
                    break;

                case PASSWORD:
                case PW:
                    passWord = value;
                    break;

                case URL:
                    url = value;
                    break;

                case RETABLE:
                    reTable = value;
                    break;

                case OUTPUTFILE:
                case OUT:
                    outputPath = value;
                    break;

                case DATELIST:
                    dateList = value;
                    break;

                case BEGINTIME:
                case BEGIN:
                    beginTime = value;
                    break;

                case ENDTIME:
                case END:
                    endTime = value;
                    break;

                case SRCNAME:
                    srcName = value;
                    break;

                case HELP:
                    parsedArgs.add(opt);
                    break;

                default:
                    parsedArgs.add(opt);
                    if(value != null){
                        parsedArgs.add(value);
                    }
                    break;
            }

            return true;
        }

        @Override
        protected boolean handleUnknown(String opt) {

            if(errorOnUnknownArgs){
                checkArgument(!opt.startsWith("-"),"Unrecognized option: %s", opt);
                return false;
            }
            return true;
        }

        @Override
        protected void handleExtraArgs(List<String> extra) {
            appArgs.addAll(extra);
        }
    }



}
