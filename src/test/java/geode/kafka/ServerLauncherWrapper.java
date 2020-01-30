package geode.kafka;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;

import java.io.IOException;
import java.util.Properties;

import static geode.kafka.GeodeKafkaTestCluster.TEST_REGION_FOR_SINK;
import static geode.kafka.GeodeKafkaTestCluster.TEST_REGION_FOR_SOURCE;

public class ServerLauncherWrapper {

    public static void main(String... args) throws IOException {
//        ServerLauncher serverLauncher  = new ServerLauncher.Builder()
//                .setMemberName("server1")
////                .setServerPort(Integer.valueOf(args[0]))
////                .setServerBindAddress("localhost")
//              //  .set("locators", "localhost[10334]")
////                .set("jmx-manager", "true")
////                .set("jmx-manager-start", "true")
//                .build();
//
//        serverLauncher.start();
//        System.out.println("Geode Server Launcher complete");




        Properties properties = new Properties();
        String locatorString = "localhost[10334]";
//        String statsFile = new File(context.getOutputDir(), "stats.gfs").getAbsolutePath();
        Cache cache = new CacheFactory(properties)
//                .setPdxSerializer(new ReflectionBasedAutoSerializer("benchmark.geode.data.*"))
                .set(ConfigurationProperties.LOCATORS, locatorString)
                .set(ConfigurationProperties.NAME,
                        "server-1")
                .set(ConfigurationProperties.LOG_FILE, "/Users/jhuynh/Pivotal/geode-kafka-connector/server.log")
                .set(ConfigurationProperties.LOG_LEVEL, "info")
//               .set(ConfigurationProperties.STATISTIC_ARCHIVE_FILE, statsFile)
                .create();
        CacheServer cacheServer = cache.addCacheServer();
        cacheServer.setPort(0);
        cacheServer.start();

        //create the region
        cache.createRegionFactory(RegionShortcut.PARTITION).create(TEST_REGION_FOR_SINK);
        cache.createRegionFactory(RegionShortcut.PARTITION).create(TEST_REGION_FOR_SOURCE);
        System.out.println("starting cacheserver");
        while (true) {

        }
    }
}
