package com.example.etcd;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.watch.WatchEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/**
 * Task coordination
 */
@Component
@ConditionalOnExpression("${module.jetcd.enable:false}")
public class EtcdModule{

    @Resource
    private JetcdClient jetcdClient;

    private static final Logger logger = LoggerFactory.getLogger(EtcdModule.class);

    // Locking path, easy to record logs
    private String lockPath;

    // Timing task thread pool
    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    // The mapping of the thread and the lock object
    private final ConcurrentMap<Thread, LockData> threadData = Maps.newConcurrentMap();


    /**
     * Lock
     * @param jetcdParam
     */
    public void lock(JetcdParam jetcdParam){
        Thread currentThread = Thread.currentThread();
        LockData existsLockData = threadData.get(currentThread);
        logger.info(currentThread.getName() + "Lock ExistLockData:" + existsLockData);
                Lease leaseClient = jetcdClient.getLease();
        Lock lockClient = jetcdClient.getLock();
        //
        if (existsLockData != null && existsLockData.isLockSuccess()) {
            int lockCount = existsLockData.lockCount.incrementAndGet();
            if (lockCount < 0) {
                throw new Error("Beyond the ETCD Lock Limited Number of Number");
            }
            return;
        }
        // Create a lease, record the lease ID
        long leaseId;
        try {
            long leaseTTL = jetcdParam.getLeaseTTL();
            leaseId = leaseClient.grant(TimeUnit.NANOSECONDS.toSeconds(leaseTTL)).get().getID();
            // Renewal heartbeat cycle
            long period = leaseTTL - leaseTTL / 5;
            // Start the timing renewal
            // The timing task of renewing the lease period is delayed for the first time, and the default is 1S. According to the actual business needs
            long initialDelay = 0L;
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                        try {
                            logger.info("Renewal of the lease....");
                            leaseClient.keepAliveOnce(leaseId);
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }
                    },
                    initialDelay,
                    period,
                    TimeUnit.NANOSECONDS);

            // Lock
            LockResponse lockResponse = lockClient.lock(ByteSequence.from(jetcdParam.getLock_key().getBytes()), leaseId).get();
            if (lockResponse != null) {
                lockPath = lockResponse.getKey().toString(StandardCharsets.UTF_8);
                logger.info("Thread: {} Successful lock, lock path: {}", currentThread.getName(), lockPath);
            }

            // Success lock, set the lock object
            LockData lockData = new LockData(jetcdParam.getLock_key(), currentThread);
            lockData.lockCount.incrementAndGet();
            lockData.setLeaseId(leaseId);
            lockData.setService(scheduledExecutorService);
            threadData.put(currentThread, lockData);
            lockData.setLockSuccess(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * examine
     * @param jetcdParam
     */
    public boolean check(JetcdParam jetcdParam){
        Thread currentThread = Thread.currentThread();
        logger.info(currentThread.getName() + "Release the lock ..");
        LockData lockData = threadData.get(currentThread);
        logger.info(currentThread.getName() + " lockData " + lockData);
        if (lockData == null) {
            logger.info("Thread:" + currentThread.getName() + "No lock, lockkey:" + jetcdParam.getLock_key());
            return true;
        }else{
            return lockData.isLockSuccess();
        }
    }

    /**
     * Unlock
     * @param jetcdParam
     */
    public void unlock(JetcdParam jetcdParam){
        Lease leaseClient = jetcdClient.getLease();
        Lock lockClient = jetcdClient.getLock();
        Thread currentThread = Thread.currentThread();
        LockData lockData = threadData.get(currentThread);
        logger.info(currentThread.getName() + " lockData " + lockData);
        if (lockData == null) {
            throw new IllegalMonitorStateException("Thread:" + currentThread.getName() + "No lock, lockkey:" + jetcdParam.getLock_key());
        }
        int lockCount = lockData.lockCount.decrementAndGet();
        if (lockCount > 0) {
            return;
        }
        if (lockCount < 0) {
            throw new IllegalMonitorStateException("Thread:" + currentThread.getName() + "The number of locks is negative, lockkey:" + jetcdParam.getLock_key());
        }
        try {
            // Normal release lock
            if (lockPath != null) {
                lockClient.unlock(ByteSequence.from(lockPath.getBytes())).get();
            }
            // Close the timing task of renewal
            lockData.getService().shutdown();
            // Delete the lease
            if (lockData.getLeaseId() != 0L) {
                leaseClient.revoke(lockData.getLeaseId());
            }
        } catch (InterruptedException | ExecutionException e) {
            //e.printStackTrace();
            logger.error("Thread:" + currentThread.getName() + "Unlock failed.", e);
        } finally {
            // Remove the current thread resource
            threadData.remove(currentThread);
        }
        logger.info("Thread: {} release lock", currentThread.getName());
    }

    /**
     * Create service monitoring
     * @param jetcdParam
     */
    public JetcdResp createService(JetcdParam jetcdParam){
        JetcdResp jetcdResp = new JetcdResp();
        long leaseId= 0L;
        try {
            Long leaseTTL = jetcdParam.getLeaseTTL();
            Lease leaseClient = jetcdClient.getLease();
            KV kvClient = jetcdClient.getKv();
            Watch watchClient = jetcdClient.getWatch();
            ByteSequence key_temp = ByteSequence.from(jetcdParam.getLock_key().getBytes(StandardCharsets.UTF_8));
            ByteSequence value_temp = ByteSequence.from("[]".getBytes(StandardCharsets.UTF_8));
            if (leaseTTL!=null&&leaseTTL>0) {
                leaseId = leaseClient.grant(leaseTTL).get().getID();
                logger.debug("leaseId=={}", leaseId);
                // Renewal heartbeat cycle
                long period = leaseTTL - leaseTTL / 5;
                // Start the timing renewal
                // The timing task of renewing the lease period is delayed for the first time, and the default is 1S. According to the actual business needs
                long initialDelay = 0L;
                long finalLeaseId = leaseId;
                scheduledExecutorService.scheduleAtFixedRate(() -> {
                            try {
                                logger.debug("Renewal of leases, leaseid == {}", finalLeaseId);
                                leaseClient.keepAliveOnce(finalLeaseId);
                            } catch (Exception e) {
                                System.out.println(e.getMessage());
                            }
                        },
                        initialDelay,
                        period,
                        TimeUnit.SECONDS);

                kvClient.put(key_temp, value_temp, PutOption.newBuilder().withLeaseId(leaseId).build());
            }else{
                kvClient.put(key_temp,value_temp);
            }
            Boolean enWatch = jetcdParam.getEnWatch();
            if (enWatch!=null&&enWatch) {
                watchClient.watch(key_temp, watchResponse -> {
                    try {
                        List<WatchEvent> events = watchResponse.getEvents();
                        for (WatchEvent event : events) {
                            KeyValue keyValue = event.getKeyValue();
                            logger.debug("lease=={},key=={},cv=={},v=={},mv=={}", keyValue.getLease(),
                                    keyValue.getKey(),
                                    keyValue.getCreateRevision(),
                                    keyValue.getVersion(),
                                    keyValue.getModRevision());
                            ByteSequence value = keyValue.getValue();
                            String vals= value.toString(StandardCharsets.UTF_8);
                            Set<Object> se = JSONObject.parseObject(vals, HashSet.class);
                            for (Object o : se) {
                                JSONObject map = (JSONObject)o;
                                String name = map.getString("name");
                                String health_check = map.getString("health_check");
                                String service_url = map.getString("service_url");
                                logger.info("name = {},health_check = {},service_url = {}",name,health_check,service_url);
                            }
                        }
                    } catch (Exception e) {
                        logger.error("I found abnormal {} during monitoring" ,e.getMessage());
                    }
                });
            }
            Map<String,Object> data = new HashMap<>();
            data.put("leaseId",leaseId);
            data.put("lock_kcy",jetcdParam.getLock_key());
            jetcdResp.setData(data);
            jetcdResp.setDesc(Boolean.TRUE.equals(enWatch) ?"Services have been created and listened.":"The service has been created and the monitoring is not opened");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            jetcdResp.setData(leaseId);
            jetcdResp.setDesc("The service has failed!");
            return jetcdResp;
        }

        return jetcdResp;
    }

    /**
     * register
     * @param jetcdParam
     */
    public JetcdResp register(JetcdParam jetcdParam){
        JetcdResp jetcdResp = new JetcdResp();
        try {
            String lock_key = jetcdParam.getLock_key();
            KV kv = jetcdClient.getKv();
            ByteSequence from = ByteSequence.from(lock_key.getBytes(StandardCharsets.UTF_8));
            CompletableFuture<GetResponse> getResponseCompletableFuture = kv.get(from, GetOption.newBuilder().isPrefix(false).build());
            GetResponse getResponse = getResponseCompletableFuture.get();
            List<KeyValue> kvs = getResponse.getKvs();
            KeyValue keyValueTemp = null;
            for (KeyValue keyValue : kvs) {
                ByteSequence key = keyValue.getKey();
                if (key.equals(from)){
                    keyValueTemp = keyValue;
                }
            }
            assert keyValueTemp != null;
            long leaseId = keyValueTemp.getLease();
            HashSet hashSet = JSONObject.parseObject(keyValueTemp.getValue().toString(StandardCharsets.UTF_8), HashSet.class);
            hashSet.add(jetcdParam.getValue());
            String jsonString = JSONObject.toJSONString(hashSet);
            ByteSequence to = ByteSequence.from(jsonString.getBytes(StandardCharsets.UTF_8));
            PutResponse putResponse = kv.put(from, to, PutOption.newBuilder().withLeaseId(leaseId).build()).get();
            jetcdResp.setDesc("Successful service registration");
            int size = hashSet.size();
            logger.info("Registered service {}",size);
            jetcdResp.setData(putResponse);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            jetcdResp.setDesc("Failure to register for service");
        }
        return jetcdResp;
    }

    /**
     * Service discovery
     * @param jetcdParam
     */
    public JetcdResp serviceFind(JetcdParam jetcdParam){
        JetcdResp jetcdResp = new JetcdResp();
        try {
            String lock_key = jetcdParam.getLock_key();
            KV kv = jetcdClient.getKv();
            ByteSequence key = ByteSequence.from(lock_key.getBytes(StandardCharsets.UTF_8));
            List<KeyValue> kvs = kv.get(key, GetOption.newBuilder().isPrefix(true).build()).get().getKvs();
            jetcdResp.setDesc("Discovery Service");
            jetcdResp.setData(kvs);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("An exception occurs{}",e.getMessage());
            jetcdResp.setDesc(e.getMessage());

        }
        return jetcdResp;
    }
}
