package com.voyager.flink.connectors.mysql;

import com.voyager.flink.bean.PackageInfos;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.StringUtils;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MysqlConnectorTableApi {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        EnvironmentSettings env = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.enableCheckpointing(60 * 1000);
        executionEnvironment.getCheckpointConfig()
                .setCheckpointStorage(parameterTool.get("checkpoint"));

        StreamTableEnvironment tableEnve = StreamTableEnvironment.create(executionEnvironment);


        tableEnve.executeSql("CREATE TABLE `t_package_info` (\n" +
                "  `id` int NOT NULL COMMENT '主键',\n" +
                "  `code` varchar(32) NOT NULL COMMENT '套餐编码',\n" +
                "  `name` varchar(32) NOT NULL COMMENT '套餐名称',\n" +
                "  `channel` char(4) NOT NULL COMMENT '关联t_sys_area表中的area_code',\n" +
                "  `cost` varchar(32) COMMENT '资费RMB单位分',\n" +
                "  `description` varchar(1024) COMMENT '套餐描述',\n" +
                "  `month_num` int COMMENT '套餐属性 0:体验套餐，1：单月包，3：季度包，12：年包，13：自定义',\n" +
                "  `group_num` int COMMENT '套餐分组',\n" +
                "  `reserve_order_show` int COMMENT '反向订购时是否显示：1：显示，0：不显示',\n" +
                "  `reserve_change_show` int COMMENT '反向变更时是否显示：1：显示，0：不显示',\n" +
                "  `reserve_alipay_show` int COMMENT '是否显示支付宝支付，1：显示，0：不显示，',\n" +
                "  `day_num` int COMMENT '体验套餐失效时间：天，只对体验套餐有效orderSource=4，month_num=0',\n" +
                "  `package_title` varchar(32) COMMENT '套餐标题',\n" +
                "  `storage_attribution` int COMMENT '存储属性:\"0:全天存储\"和\"1:事件存储\"',\n" +
                "  `auto_renew_price` varchar(32) COMMENT '自动续费价格：微信，支付宝',\n" +
                "  `create_time` timestamp(3) COMMENT '创建时间',\n" +
                "  `update_time` timestamp(3) COMMENT '更新时间',\n" +
                "  `platform` int NOT NULL COMMENT '所属平台，1：家庭版、2：专业版',\n" +
                "  `auto_renew_type` int NOT NULL COMMENT '自动续费开关,1：开启，0：关闭;默认：0关闭',\n" +
                "  `project_code` varchar(64) COMMENT '项目编码',\n" +
                "  PRIMARY KEY (`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = '10.12.21.47',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = 'XxN9nxZ7Z$JS',\n" +
                " 'database-name' = 'platform',\n" +
                " 'table-name' = 't_package_info',\n" +
                " 'scan.incremental.snapshot.enabled' = 'false'\n" +
                ")");

        final List<String> fieldList = Arrays.stream(PackageInfos.class.getDeclaredFields())
                .map(Field::getName)
                .filter(n -> !"op".equalsIgnoreCase(n))
                .collect(Collectors.toList());
        String fields = String.join(",", fieldList);

        Table table = tableEnve.sqlQuery("select " + fields + " from t_package_info");
        DataStream<Tuple2<Boolean, RowData>> stream = tableEnve.toRetractStream(table, RowData.class);
        DataStream<String> outstream = stream.map(new MapFunction<Tuple2<Boolean, RowData>, String>() {
            @Override
            public String map(Tuple2<Boolean, RowData> value) throws Exception {
                StringBuilder b = new StringBuilder();
                b.append(value.f0 ? "INSERT" : "DELETE");
                char delimiter = '\007';
                b.append(delimiter);
                Field fields1 = GenericRowData.class.getDeclaredField("fields");
                fields1.setAccessible(true);
                Object[] fs = (Object[]) fields1.get((GenericRowData) value.f1);
                for (int i = 0; i < fieldList.size(); i++) {
                    b.append(StringUtils.arrayAwareToString(fs[i]));
                    b.append(i == fieldList.size() - 1 ? "" : delimiter);
                }
                return b.toString();
            }
        });

        outstream.print();

        executionEnvironment.execute();
    }
}
