package org.example;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.RLock;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.TimeUnit;

@RestController
@Slf4j
public class RedisController {

    @Autowired
    RedissonClient redissonClient;

    @Autowired
    ServerConfig serverConfig;
    @Autowired
    RestTemplate restTemplate;

    @RequestMapping("/{jobId}")
    public void job(@PathVariable("jobId") String jobId) {
        if (redissonClient.getBucket(jobId).isExists()) {
            return;
        }
        RScoredSortedSet<Object> scoredSortedSet = redissonClient.getScoredSortedSet("debezium-balance");
        String firstRedisFlag = (String) scoredSortedSet.first();
        if (firstRedisFlag == null) {
            return;
        }
        String url = "http://" + (String) redissonClient.getBucket(firstRedisFlag).get();
        for (int i = 0; i < 3; i++) {
            try {
                String isOk = restTemplate.getForObject(url + "/check", String.class);
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

        for (int i = 0; i < 3; i++) {
            try {
                String isOk = restTemplate.getForObject(url + "/addJob/" + jobId, String.class);
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
    }


    @RequestMapping("/addJob/{jobId}")
    public String addJob(@PathVariable("jobId") String jobId) {
        if (redissonClient.getBucket(jobId).isExists()) {
            return "FAIL";
        }
        new MySqlApiCdc1(serverConfig, redissonClient, restTemplate).run(jobId);
        new MySqlApiCdc1(serverConfig, redissonClient, restTemplate).run2(jobId + 1000);
        return "OK";
    }

    @RequestMapping("/")
    public String testRedisson() {
        //获取分布式锁，只要锁的名字一样，就是同一把锁
        RLock lock = redissonClient.getLock("lock");

        //加锁（阻塞等待），默认过期时间是无限期
        try {
            if (lock.tryLock(15, TimeUnit.SECONDS)) {

            }
            //如果业务执行过长，Redisson会自动给锁续期
            System.out.println("加锁成功，执行业务逻辑");
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            //解锁，如果业务执行完成，就不会继续续期
            lock.unlock();
        }

        return "Hello Redisson!";
    }

    @RequestMapping("/check/{brockId}")
    String check(@PathVariable("brockId") String brockId) {
        if (StringUtils.equals(brockId, CdcConfig.FLAG)) {
            return "OK";
        }
        return "FAIL";
    }
}
