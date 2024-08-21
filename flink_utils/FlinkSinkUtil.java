package com.hwc.gmall.realtime.common.utils;

import com.alibaba.fastjson.JSONObject;
import com.hwc.gmall.realtime.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

// Flink最后一步输出，获取Sink
public class FlinkSinkUtil {
    public static KafkaSink<String> getKafkaSink(String topicName) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
                        .setTopic(topicName)
                        // 发数据肯定不会有空值
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("hwc-" + topicName + System.currentTimeMillis())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();
    }

    // 根据数据流中的sink_table字段，对应的输出到名字一样的Kafka主题
    // aaa表 -> topicName = aaa
    public static KafkaSink<JSONObject> autoChooseKafkaTopicSink() {
        return KafkaSink.<JSONObject>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<JSONObject>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject s, KafkaSinkContext kafkaSinkContext, Long aLong) {
                        String topicName = s.getString("sink_table");
                        s.remove("sink_table");
                        return new ProducerRecord<>(topicName, Bytes.toBytes(s.toJSONString()));
                    }
                })
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("hwc-" + "base_db" + System.currentTimeMillis())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();
    }

    public static DorisSink<String> getDorisSink(String tableName) {
        Properties properties = new Properties();
        // 上游是 json 写入时，需要开启配置
        properties.setProperty("format", "json");
        // read_json_by_line 必须要一条数据(一行)一个JSON
        properties.setProperty("read_json_by_line", "true");

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder()
                        .setLabelPrefix("label-doris" + System.currentTimeMillis())
                        .setDeletable(false)
                        .setStreamLoadProp(properties).build())
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes(Constant.FENODES)
                        .setTableIdentifier(Constant.DORIS_DATABASE + "." + tableName)
                        .setUsername(Constant.DORIS_USER_NAME)
                        .setPassword(Constant.DORIS_PASSWORD)
                        .build())
                .build();
    }
}
