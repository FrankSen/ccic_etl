package net.ccic.etl.launcher;

import java.util.List;

/**
 * @Description
 * @Author franksen
 * @CREATE 2019-08-20-下午6:25
 */
public class MainClassOptionParser extends EtlSubmitOptionParser {

    String className;

    @Override
    protected boolean handle(String opt, String value) {
        if (CLASS.equals(opt)) {
            className = value;
        }
        return false;
    }

    @Override
    protected boolean handleUnknown(String opt) {
        return false;
    }

    @Override
    protected void handleExtraArgs(List<String> extra) {

    }
}
