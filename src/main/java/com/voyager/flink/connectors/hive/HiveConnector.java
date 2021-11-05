package com.voyager.flink.connectors.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class HiveConnector {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name = "flink-hive";
        String defaultDatabase = "flink-default";
        String hiveConfDir = "classpath:hive-site.xml";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("flink-hive", hive);

        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("flink-hive");
    }
}
