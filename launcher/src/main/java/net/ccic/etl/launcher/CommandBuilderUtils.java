package net.ccic.etl.launcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Author franksen
 * @CREATE 2019-08-08-下午5:07
 */
 class CommandBuilderUtils {

    /**Throw IllegalArgumentException with the given message if the check is false*/
    static void checkArgument(boolean check, String msg, Object... args){
        if(!check){
            throw new IllegalArgumentException(String.format(msg, args));
        }
    }

    /**Return whether the given string is null or empty. */
    static boolean isEmpty(String obj){
        return obj == null || obj.isEmpty();
    }

    static void  checkState(boolean check, String msg, Object... args){
        if(!check){
            throw new IllegalArgumentException(String.format(msg, args));
        }
    }

     static String join(String sep, Iterable<String> cmd) {
        StringBuilder sb = new StringBuilder();
        for (String e : cmd) {
            if(e != null){
                if(sb.length() > 0){
                    sb.append(sep);
                }
                sb.append(e);
            }

        }
        return sb.toString();
    }

    static String join(String sep, String... elements) {
        StringBuilder sb = new StringBuilder();
        for (String e : elements) {
            if (e != null) {
                if (sb.length() > 0) {
                    sb.append(sep);
                }
                sb.append(e);
            }
        }
        return sb.toString();
    }

    static boolean isWindows() {
        String os = System.getProperty("os.name");
        return os.startsWith("Windows");
    }

     static String prepareWindowsCommand(List<String> cmd, Map<String, String> childEnv) {
        StringBuilder cmdline = new StringBuilder();
        for (Map.Entry<String, String> e : childEnv.entrySet()) {
            cmdline.append(String.format("set %s=%s", e.getKey(), e.getValue()));
            cmdline.append(" && ");
        }
        for (String arg : cmd) {
            cmdline.append(quoteForBatchScript(arg));
            cmdline.append(" ");
        }
        return cmdline.toString();
    }

      static String quoteForBatchScript(String arg) {

        boolean needsQuotes = false;
        for (int i = 0; i < arg.length(); i++) {
            int c = arg.codePointAt(i);
            if (Character.isWhitespace(c) || c == '"' || c == '=' || c == ',' || c == ';') {
                needsQuotes = true;
                break;
            }
        }
        if (!needsQuotes) {
            return arg;
        }
        StringBuilder quoted = new StringBuilder();
        quoted.append("\"");
        for (int i = 0; i < arg.length(); i++) {
            int cp = arg.codePointAt(i);
            switch (cp) {
                case '"':
                    quoted.append('"');
                    break;

                default:
                    break;
            }
            quoted.appendCodePoint(cp);
        }
        if (arg.codePointAt(arg.length() - 1) == '\\') {
            quoted.append("\\");
        }
        quoted.append("\"");
        return quoted.toString();
    }


    static List<String> prepareBashCommand(List<String> cmd, Map<String, String> childEnv) {
        if (childEnv.isEmpty()) {
            return cmd;
        }

        List<String> newCmd = new ArrayList<>();
        newCmd.add("env");

        for (Map.Entry<String, String> e : childEnv.entrySet()) {
            newCmd.add(String.format("%s=%s", e.getKey(), e.getValue()));
        }
        newCmd.addAll(cmd);
        return newCmd;
    }
}
