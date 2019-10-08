package net.ccic.etl.launcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Author franksen
 * @CREATE 2019-08-10-下午9:39
 */
public class EtlClassCommandBuilder extends AbstractCommandBuilder {

    final List<String> appArgs ;

    public EtlClassCommandBuilder(List<String> appArgs) {
        this.appArgs = appArgs;
    }

    @Override
    List<String> buildCommand(Map<String, String> env) throws IOException, IllegalArgumentException {
        return null;
    }
}
