package geode.kafka;

import java.io.IOException;

public class GeodeLocalCluster {

    private JavaProcess locatorProcess;
    private JavaProcess serverProcess;

    public GeodeLocalCluster() {
        locatorProcess = new JavaProcess(LocatorLauncherWrapper.class);
        serverProcess = new JavaProcess(ServerLauncherWrapper.class);
    }

    public void start() throws IOException, InterruptedException {
        System.out.println("starting locator");
        locatorProcess.exec("10334");
        Thread.sleep(15000);
        System.out.println("is alive?" + locatorProcess.process.isAlive());
        serverProcess.exec("40404");
        Thread.sleep(30000);
    }

    public void stop() {
        serverProcess.destroy();
        locatorProcess.destroy();
    }
}

