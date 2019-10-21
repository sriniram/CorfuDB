package org.corfudb.browser;

import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.CorfuRuntimeHelper;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

/**
 * Main class for the CorfuStore Browser Tool.
 * Command line options are documented in the USAGE variable.
 * <p>
 * - Created by pmajmudar on 10/16/2019
 */
@Slf4j
public class CorfuStoreBrowserMain {
    private static final String USAGE = "Usage: corfu-browser --host=<host> " +
            "--port=<port> --namespace=<namespace> --tablename=<tablename> " +
            "[--keystore=<keystore_file>] [--ks_password=<keystore_password>] " +
            "[--truststore=<truststore_file>] [--truststore_password=<truststore_password>] " +
            "[--tlsEnabled=<tls_enabled>]\n"
            + "Options:\n"
            + "--host=<host>   Hostname\n"
            + "--port=<port>   Port\n"
            + "--namespace=<namespace>   Namespace\n"
            + "--tablename=<tablename>   Table Name\n"
            + "--keystore=<keystore_file> KeyStore File\n"
            + "--ks_password=<keystore_password> KeyStore Password\n"
            + "--truststore=<truststore_file> TrustStore File\n"
            + "--truststore_password=<truststore_password> Truststore Password\n"
            + "--tlsEnabled=<tls_enabled>";

    public static void main(String[] args) {
        try {
            // Parse the options given, using docopt.
            Map<String, Object> opts =
                    new Docopt(USAGE)
                            .withVersion(GitRepositoryState.getRepositoryState().describe)
                            .parse(args);

            CorfuRuntime runtime = CorfuRuntimeHelper.getRuntimeForArgs(opts);
            CorfuStoreBrowser browser = new CorfuStoreBrowser(runtime);
            CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table = browser.getTable(
                    opts.get("--namespace").toString(),
                    opts.get("--tablename").toString()
            );
            browser.printTable(table);
        } catch (Throwable t) {
            log.error("Error in Browser Execution.", t);
            throw t;
        }
    }
}
