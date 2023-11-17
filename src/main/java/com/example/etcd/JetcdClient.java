package com.example.etcd;

import io.etcd.jetcd.*;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Component
@Getter
@Setter
public class JetcdClient {

    @Value("#{'${module.jetcd.endpoints:}'.split(',')}")
    private String[] endpoints;

    private Client client;

    private KV kv;

    private Lock lock;

    private Lease lease;

    private Watch watch;

    private Election election;

    @PostConstruct
    public void init() {
        client=Client.builder().endpoints(endpoints).build();
        kv=client.getKVClient();
        lock=client.getLockClient();
        lease=client.getLeaseClient();
        watch=client.getWatchClient();
        election=client.getElectionClient();
    }

    @PreDestroy
    public void destroy() {
        election.close();
        kv.close();
        lease.close();
        watch.close();
        lock.close();
        client.close();
    }

    // getter() & setter() ...
}
