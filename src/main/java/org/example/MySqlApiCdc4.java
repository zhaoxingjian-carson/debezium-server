package org.example;

import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.RLock;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


//@Component
@Slf4j
public class MySqlApiCdc4 {


    @Autowired
    RedissonClient redissonClient;

    private DebeziumEngine<ChangeEvent<String, String>> engine;
    RLock lock;

    @PreDestroy
    public void preDestroy() {
        if (this.engine != null) {
            try {
                this.engine.close();
            } catch (IOException ignore) {
            }
            log.info("o=|>> 关闭debezium完成 <<|=o");
        }
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }

    @PostConstruct
    public void init() {

        engine = DebeziumEngine
                .create(Json.class).using(getConfiguration().asProperties()).notifying(record -> {
                    System.out.println("test2 " + record.value());
                }).build();

        // Run the engine asynchronously ...

        // Run the engine asynchronously ...
        Executor executor = Executors.newSingleThreadExecutor((runnable) -> new Thread(runnable, "debezium-engine-executor2"));
        executor.execute(engine);
        log.info("carson debezium2 start");

    }

    public static Configuration getConfiguration() {
        int random = new Random().nextInt(1000);

        return Configuration.create()
                /* begin engine properties */
                .with("name", "zt3000-debezuim-mysql-connector" + random)
                .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", "D:/storage/offset" + random + ".dat")
                .with("offset.flush.interval.ms", 60000)
                .with("snapshot.mode", "when_needed") //initial/when_needed/schema_only/schema_only_recovery/never
                .with("snapshot.locking.mode", "minimal") //minimal/minimal_percona/extended/none
                .with("snapshot.include.collection.list", "All tables specified in table.include.list")
                .with("signal.data.collection", "debezium.debezium_signal")
                /* begin connector properties
                 * https://debezium.io/documentation/reference/2.1/connectors/mysql.html#mysql-property-column-propagate-source-type
                 */
                .with("database.hostname", "192.168.131.129")
                .with("database.port", 3306)
                .with("database.user", "root")
                .with("database.password", "root")
                .with("database.server.id", random)
                .with("topic.prefix", "zt3000-debezuim-mysql-connector2")
                .with("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory")
                .with("schema.history.internal.file.filename", "D:/storage/schemahistory" + random + ".dat")
                .with("database.include.list", "order")
                .with("table.include.list", "order.test")
                .with("include.schema.changes", true)
                .with("include.query", false) // binlog_rows_query_log_events option set to ON
                .with("max.batch.size", 1024)
                .with("max.queue.size", 8192)
                .with("poll.interval.ms", 100)
                .with("connect.timeout.ms", 30000)
                .with("message.key.columns", "test_binlog.tb_metadata_plat_cat:name,type") //inventory.customers:pk1,pk2;(.*).purchaseorders:pk3,pk4
                .with("snapshot.fetch.size", 1024) //During a snapshot, the connector reads table content in batches of rows. This property specifies the maximum number of rows in a batch.
                .build();
    }
}