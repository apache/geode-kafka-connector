package geode.kafka;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.LocatorLauncher;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

public class LocatorLauncherWrapper {

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
//        String statsFile = new File(context.getOutputDir(), "stats.gfs").getAbsolutePath();
//        properties.setProperty(ConfigurationPropert/**/ies.STATISTIC_ARCHIVE_FILE, statsFile);
        properties.setProperty(ConfigurationProperties.NAME, "locator1");

        Locator.startLocatorAndDS(10334, new File("/Users/jhuynh/Pivotal/geode-kafka-connector/locator.log"), properties);
        while (true) {

        }
//
//        LocatorLauncher locatorLauncher  = new LocatorLauncher.Builder()
//                .setMemberName("locator1")
////                .setPort(Integer.valueOf(args[0]))
////                .setBindAddress("localhost")
//                .build();
//
//        locatorLauncher.start();
//        while (!locatorLauncher.isRunning()) {
//
//        }
//        System.out.println(locatorLauncher.getBindAddress() + ":" + locatorLauncher.getPort());

    }
}
