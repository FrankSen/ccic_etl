package net.ccic.etl.launcher;

import java.io.IOException;
import java.util.*;

import static net.ccic.etl.launcher.CommandBuilderUtils.*;

/**
 * @Description
 * @Author franksen
 * @CREATE 2019-08-08-下午5:04
 */
class Main {

    public static void main(String[] argsArry) throws IOException {
        checkArgument(argsArry.length > 0, "Not enough arguments: missing class name.");

        List<String> args = new ArrayList<>(Arrays.asList(argsArry));
        String className = args.remove(0);

        boolean printLaunchCommand = !isEmpty(System.getenv("ETL_PRINT_LANGUAGE_COMMAND"));

        Map<String,String> env = new HashMap<>();
        List<String> cmd ;

        if(className.equals("net.ccic.etl.submit.EtlSubmit")){

            try{
                AbstractCommandBuilder builder = new EtlSubmitCommandBuilder(args, className);
                cmd = buildCommand(builder, env, printLaunchCommand);
            }catch (IllegalArgumentException e){
                cmd = catchModel(env, e);

            }

            generateCommand(env, cmd);

        }else{
            AbstractCommandBuilder etlSubmitCommandBuilder = new EtlClassCommandBuilder(args);
            cmd = buildCommand(etlSubmitCommandBuilder, env, printLaunchCommand);

            generateCommand(env,cmd);
        }

    }

    private static List<String> catchModel(Map<String, String> env, IllegalArgumentException e) throws IOException {
        boolean printLaunchCommand;
        List<String> cmd;
        printLaunchCommand = false;
        System.err.println("Error: " + e.getMessage());
        System.err.println();

        MainClassOptionParser parser = new MainClassOptionParser();
        List<String> help = new ArrayList<>();
        if(parser.className != null){
            help.add(parser.CLASS);
            help.add(parser.className);
        }
        help.add(parser.USAGE_ERROR);
        AbstractCommandBuilder builder = new EtlSubmitCommandBuilder(help);
        cmd = buildCommand(builder, env, printLaunchCommand);
        return cmd;
    }


    private static void generateCommand(Map<String, String> env, List<String> cmd) {
        if (isWindows()) {
            System.out.println(prepareWindowsCommand(cmd, env));
        } else {
            // In bash, use NULL as the arg separator since it cannot be used in an argument.
            List<String> bashCmd = prepareBashCommand(cmd, env);
            for (String c : bashCmd) {
                System.out.print(c);
                System.out.print('\0');
            }
        }
    }

    private static List<String> buildCommand(
            AbstractCommandBuilder builder,
            Map<String, String> env,
            boolean printLaunchCommand) throws IOException, IllegalArgumentException {
        List<String> cmd = builder.buildCommand(env);
        if(printLaunchCommand){
            System.err.println("CCIC_ETL command:" + join(" ",cmd));
            System.err.println("========================================");
        }
        return cmd;
    }
}
