package com.example.etcd;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
@Setter
public class LockData {

    public AtomicInteger lockCount = new AtomicInteger(0);

    private long leaseId;

    private ScheduledExecutorService service;

    private Thread currentThread;

    private String lockKey;

    private boolean lockSuccess;

    public LockData(String lockKey,Thread currentThread){
        this.lockKey = lockKey;
        this.currentThread = currentThread;
    }

    // getter() & setter() ...
}
