package org.corfudb.universe.node.server.process;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.node.server.CorfuServerParams;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Provides executable commands for the CorfuServer-s to create a directory structure,
 * copy a corfu server jar file from the source to the target directory,
 * to manage corfu server (start, stop, kill)
 */
@Slf4j
public class CorfuProcessManager {

    @NonNull
    private final CorfuServerParams params;

    @Getter
    @NonNull
    private final Path corfuDir;
    private final Path serverDir;
    private final Path dbDir;
    @Getter
    private final Path serverJar;
    private final Path serverJarRelativePath;

    public CorfuProcessManager(Path corfuDir, @NonNull CorfuServerParams params) {

        this.corfuDir = corfuDir;
        this.params = params;

        serverDir = corfuDir.resolve(params.getName());
        dbDir = corfuDir.resolve(params.getStreamLogDir());

        serverJarRelativePath = Paths.get(params.getName(), "corfu-server.jar");
        serverJar = corfuDir.resolve(serverJarRelativePath);
    }

    public String createServerDirCommand() {
        return "mkdir -p " + serverDir;
    }

    public String createStreamLogDirCommand() {
        return "mkdir -p " + dbDir;
    }

    public String pauseCommand() {
        return "ps -ef" +
                " | " +
                "grep -v grep" +
                " | " +
                String.format("grep \"%s\"", params.getName()) +
                " | " +
                "awk '{print $2}'" +
                " | " +
                "xargs kill -STOP";
    }

    public String startCommand(String commandLineParams) {
        Path corfuLog = serverDir.resolve("corfu.log");

        // Compose command line for starting Corfu
        return "java -cp " +
                serverJarRelativePath +
                " " +
                org.corfudb.infrastructure.CorfuServer.class.getName() +
                " " +
                commandLineParams +
                " > " +
                corfuLog +
                " 2>&1 &";
    }

    public String resumeCommand() {
        log.info("Resuming the corfu server: {}", params.getName());

        return "ps -ef" +
                " | " +
                "grep -v grep" +
                " | " +
                String.format("grep \"%s\"", params.getName()) +
                " | " +
                "awk '{print $2}'" +
                " | " +
                "xargs kill -CONT";
    }

    public String stopCommand() {
        log.info("Stop corfu server. Params: {}", params);

        return "ps -ef" +
                " | " +
                "grep -v grep" +
                " | " +
                String.format("grep \"%s\"", params.getName()) +
                " | " +
                "awk '{print $2}'" +
                " | " +
                "xargs kill -15";
    }

    public String killCommand() {
        log.info("Kill the corfu server. Params: {}", params);
        return "ps -ef" +
                " | " +
                "grep -v grep" +
                " | " +
                String.format("grep \"%s\"", params.getName()) +
                " | " +
                "awk '{print $2}'" +
                " | " +
                "xargs kill -9";
    }

    public String removeServerDirCommand() {
        return String.format("rm -rf %s", serverDir);
    }
}
