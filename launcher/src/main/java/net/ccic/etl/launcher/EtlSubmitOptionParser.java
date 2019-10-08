package net.ccic.etl.launcher;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Description
 * @Author franksen
 * @CREATE 2019-08-09-下午5:37
 */
class EtlSubmitOptionParser {

    protected final String CLASS = "--class";
    protected final String SQLTEXT = "--sql";
    protected final String RUN_MODE = "--run-mode";
    protected final String APPNAME = "--name";
    protected final String LOGPATH = "--log";
    protected final String SRCNAME = "--src";
    protected final String PRIMKEY = "--key";
    protected final String HELP = "--help";
    protected final String HLP = "-h";
    protected final String VERBOSE = "--verbose";
    protected final String VB = "-v";
    protected final String USERNAME = "--username";
    protected final String USER = "-u";
    protected final String PASSWORD = "--password";
    protected final String PW = "-p";
    protected final String URL = "--url";
    protected final String RETABLE = "--rt";
    protected final String BEGINTIME = "--start";
    protected final String BEGIN = "-s";
    protected final String ENDTIME = "--end";
    protected final String END = "-e";
    protected final String OUTPUTFILE = "--output-file";
    protected final String OUT = "-o";
    protected final String DATELIST = "--date-list";
    protected final String INTERVAL = "--interval";


    protected final String USAGE_ERROR = "--usage-error";
    final String [][] opts = {
            {APPNAME}
            ,{CLASS}
            ,{RUN_MODE}
            ,{LOGPATH}
            ,{SRCNAME}
            ,{SQLTEXT}
            ,{PRIMKEY}
            ,{USERNAME, USER}
            ,{PASSWORD, PW}
            ,{BEGINTIME, BEGIN}
            ,{ENDTIME, END}
            ,{OUTPUTFILE, OUT}
            ,{URL}
            ,{RETABLE}
            ,{DATELIST}
            ,{INTERVAL}
    };

    final String [][] switches = {
            {HELP , HLP}
            ,{VERBOSE, VB}
            ,{USAGE_ERROR}
    };


    protected final void parse(List<String> args) {
        Pattern eqSeparatedOpt = Pattern.compile("(--[^=]+)=(.+)");

        int idx = 0;
        for (idx = 0; idx < args.size(); idx++) {
            String arg = args.get(idx);
            String value = null;

            Matcher m = eqSeparatedOpt.matcher(arg);
            if (m.matches()) {
                arg = m.group(1);
                value = m.group(2);
            }

            // Look for options with a value.
            String name = findCliOption(arg, opts);
            if (name != null) {
                if (value == null) {
                    if (idx == args.size() - 1) {
                        throw new IllegalArgumentException(
                                String.format("Missing argument for option '%s'.", arg));
                    }
                    idx++;
                    value = args.get(idx);
                }
                if (!handle(name, value)) {
                    break;
                }
                continue;
            }

            // Look for a switch that no take value.
            name = findCliOption(arg, switches);
            if (name != null) {
                if (!handle(name, null)) {
                    break;
                }
                continue;
            }

            if (!handleUnknown(arg)) {
                break;
            }
        }

        if (idx < args.size()) {
            idx++;
        }
        handleExtraArgs(args.subList(idx, args.size()));
    }


    private String findCliOption(String name, String[][] available) {
        for (String[] candidates : available) {
            if (candidates.length >= 2){

                for (String candidate : candidates) {
                    if (candidate.equals(name)) {
                        return candidates[1];
                    }
                }
            }else{
                for (String candidate : candidates) {
                    if (candidate.equals(name)) {
                        return candidates[0];
                    }
                }
            }
        }
        return null;
    }

    protected boolean handle(String opt, String value) {
        throw new UnsupportedOperationException();
    }

    protected boolean handleUnknown(String opt) {
        throw new UnsupportedOperationException();
    }

    protected void handleExtraArgs(List<String> extra) {
        throw new UnsupportedOperationException();
    }

}
