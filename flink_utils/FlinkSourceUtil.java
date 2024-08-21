package com.hwc.gmall.realtime.common.utils;

import com.hwc.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static com.hwc.gmall.realtime.common.constant.Constant.KAFKA_BROKERS;

public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String groupId,
                                                     String topic) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setGroupId(groupId)
                .setValueOnlyDeserializer(
                        // SimpleStringSchema无法反序列化null数据，会报错
                        // 后续DWD层会想kafka发送空值 -D
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] bytes) throws IOException {
                                if (bytes != null && bytes.length != 0) {
                                    return new String(bytes, StandardCharsets.UTF_8);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return BasicTypeInfo.STRING_TYPE_INFO;
                            }
                        }
                )
                .build();
    }

    public static MySqlSource<String> FlinkCDCToGetMySQLSource(String dataBaseName, String tableName) {
        // 预防MySQL无法连接
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList(dataBaseName)
                .tableList(dataBaseName + "." + tableName) // gmall2024_config.table_process_dim
                .jdbcProperties(props)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())// 初始化读取，先读取全部，后续再读取变更数据
                .build();
        return mySqlSource;
    }
}
