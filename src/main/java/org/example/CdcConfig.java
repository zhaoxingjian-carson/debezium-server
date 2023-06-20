package org.example;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;


@Configuration
@Slf4j
public class CdcConfig implements ApplicationListener<AvailabilityChangeEvent> {


    @Autowired
    RedissonClient redissonClient;

    @Autowired
    ServerConfig serverConfig;


    volatile int intiCount = 0;

    public static String FLAG = UUID.randomUUID().toString();

    public static Map<String, DebeziumEngine<ChangeEvent<String, String>>> engineMap = new ConcurrentHashMap<>();

    public static Map<String, RLock> lockMap = new ConcurrentHashMap<>();

    //    public static String FLAG = "871a601c-5d26-4f1c-bbb6-9ab2a4d94e41qw1";
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

    @PreDestroy
    public void preDestroy() {
        engineMap.forEach(new BiConsumer<String, DebeziumEngine<ChangeEvent<String, String>>>() {
            @Override
            public void accept(String jobId, DebeziumEngine<ChangeEvent<String, String>> engine) {
                if (engine != null) {
                    try {
                        engine.close();
                    } catch (IOException ignore) {
                    }
                    log.info("o=|>> 关闭debezium完成 <<|=o");
                }
            }
        });
        lockMap.forEach(new BiConsumer<String, RLock>() {
            @Override
            public void accept(String jobId, RLock rLock) {
                if (rLock.isHeldByCurrentThread()) {
                    rLock.forceUnlock();
                }
            }
        });

    }

}
