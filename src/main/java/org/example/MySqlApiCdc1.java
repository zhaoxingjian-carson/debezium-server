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
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


//@Component
@Slf4j
public class MySqlApiCdc1 implements ApplicationListener<AvailabilityChangeEvent> {

    ServerConfig serverConfig;

    RedissonClient redissonClient;
    RestTemplate restTemplate;

    private DebeziumEngine<ChangeEvent<String, String>> engine;

    RLock lock;

    volatile int intiCount = 0;


    public MySqlApiCdc1(ServerConfig serverConfig, RedissonClient redissonClient, RestTemplate restTemplate) {
        this.serverConfig = serverConfig;
        this.redissonClient = redissonClient;
        this.restTemplate = restTemplate;
    }

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

    @Override
    public void onApplicationEvent(AvailabilityChangeEvent event) {
        if (intiCount == 0) {
            intiCount = 1;
            RScoredSortedSet<Object> scoredSortedSet = redissonClient.getScoredSortedSet("debezium-balance");
            scoredSortedSet.addScore(CdcConfig.FLAG, 1);
            redissonClient.getBucket(CdcConfig.FLAG).set(serverConfig.getUrl());
//            init();
        }
    }

    //    @PostConstruct
    public void init() {
        lock = redissonClient.getLock("debezium-lock3");
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    //获取分布式锁，只要锁的名字一样，就是同一把锁
                    //加锁（阻塞等待），默认过期时间是无限期
                    try {
                        if (!lock.tryLock(5, TimeUnit.SECONDS)) {
                            log.info("carson debezium1 sleep");
                            continue;
                        }

                        // 负载均衡，存活检查
                        RScoredSortedSet<Object> scoredSortedSet = redissonClient.getScoredSortedSet("debezium-balance");
                        boolean exit = scoredSortedSet.contains(CdcConfig.FLAG);
                        if (!exit) {
                            // 不存在，直接加入
                            scoredSortedSet.addScore(CdcConfig.FLAG, 1);
                            redissonClient.getBucket(CdcConfig.FLAG).set(serverConfig.getUrl());
                        } else {
                            String firstRedisFlag = (String) scoredSortedSet.first();
                            if (StringUtils.endsWithIgnoreCase(CdcConfig.FLAG, firstRedisFlag)) {
                                // 最小的那个
                                scoredSortedSet.addScore(CdcConfig.FLAG, 1);
                                redissonClient.getBucket(CdcConfig.FLAG).set(serverConfig.getUrl());
                            } else {
                                String url = (String) redissonClient.getBucket(firstRedisFlag).get();
                                for (int i = 0; i < 3; i++) {
                                    try {
                                        String isOk = restTemplate.getForObject("http://" + url + "/check", String.class);
                                        if (!"OK".equals(isOk)) {
                                            throw new RestClientException("");
                                        } else {
                                            break;
                                        }
                                    } catch (Exception e) {
                                        log.info("", e);
                                        if (i == 2) {
                                            scoredSortedSet.pollFirst();
                                        }
                                        continue;
                                    }
                                }
                                log.info("carson debezium1");
                                lock.unlock();
                                Thread.sleep(1000 * 10);
                                continue;
                            }
                        }

                        engine = DebeziumEngine
                                .create(Json.class).using(getConfiguration("").asProperties()).notifying(record -> {
                                    System.out.println("test1 " + record.value());
                                }).build();

                        // Run the engine asynchronously ...

                        // Run the engine asynchronously ...
                        Executor executor = Executors.newSingleThreadExecutor((runnable) -> new Thread(runnable, "debezium-engine-executor2"));
                        executor.execute(engine);
                        log.info("carson debezium1 start");
                        break;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    public static Configuration getConfiguration(String jobId) {

        return Configuration.create()
                /* begin engine properties */
                .with("name", "zt3000-debezuim-mysql-connector" + jobId)
                .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", "D:/storage/offset" + jobId + ".dat")
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
                .with("database.server.id", jobId)
                .with("topic.prefix", "zt3000-debezuim-mysql-connector" + jobId)
                .with("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory")
                .with("schema.history.internal.file.filename", "D:/storage/schemahistory" + jobId + ".dat")
                .with("database.include.list", "user")
                .with("table.include.list", "user.user")
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

    public static Configuration getConfiguration2(String jobId) {

        return Configuration.create()
                /* begin engine properties */
                .with("name", "zt3000-debezuim-mysql-connector" + jobId)
                .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", "D:/storage/offset" + jobId + ".dat")
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
                .with("database.server.id", jobId)
                .with("topic.prefix", "zt3000-debezuim-mysql-connector" + jobId)
                .with("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory")
                .with("schema.history.internal.file.filename", "D:/storage/schemahistory" + jobId + ".dat")
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

    int checkCount = 0;

    public void run(String jobId) {
        lock = redissonClient.getLock("debezium-lock-" + jobId);
        checkCount = 0;
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        if (checkCount >= 10) {
                            break;
                        }
                        if (!lock.tryLock(10, TimeUnit.SECONDS)) {
                            log.info("carson debezium sleep" + jobId);
                            continue;
                        }

                        RScoredSortedSet<Object> scoredSortedSet = redissonClient.getScoredSortedSet("debezium-balance");
                        scoredSortedSet.addScore(CdcConfig.FLAG, 1);

                        CdcConfig.lockMap.put(jobId, lock);

                        engine = DebeziumEngine
                                .create(Json.class).using(getConfiguration(jobId).asProperties()).notifying(record -> {
                                    System.out.println("test" + jobId + "   " + record.value());
                                }).build();

                        // Run the engine asynchronously ...
                        Executor executor = Executors.newSingleThreadExecutor((runnable) -> new Thread(runnable, "debezium-engine-executor" + jobId));
                        executor.execute(engine);
                        CdcConfig.engineMap.put(jobId, engine);
                        log.info("carson debezium start" + jobId);
                        break;

                    } catch (Exception e) {

                    }
                }
            }
        });
        thread.setDaemon(true);
        thread.start();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
//        run2(jobId + 1000);
    }

    public void run2(String jobId) {
        lock = redissonClient.getLock("debezium-lock-" + jobId);
        checkCount = 0;
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        if (checkCount >= 10) {
                            break;
                        }
                        if (!lock.tryLock(10, TimeUnit.SECONDS)) {
                            log.info("carson debezium sleep" + jobId);
                            continue;
                        }

                        RScoredSortedSet<Object> scoredSortedSet = redissonClient.getScoredSortedSet("debezium-balance");
                        scoredSortedSet.addScore(CdcConfig.FLAG, 1);

                        CdcConfig.lockMap.put(jobId, lock);

                        engine = DebeziumEngine
                                .create(Json.class).using(getConfiguration2(jobId).asProperties()).notifying(record -> {
                                    System.out.println("test" + jobId + "   " + record.value());
                                }).build();

                        // Run the engine asynchronously ...
                        Executor executor = Executors.newSingleThreadExecutor((runnable) -> new Thread(runnable, "debezium-engine-executor" + jobId));
                        executor.execute(engine);
                        CdcConfig.engineMap.put(jobId, engine);
                        log.info("carson debezium start" + jobId);
                        break;

                    } catch (Exception e) {

                    }
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

}