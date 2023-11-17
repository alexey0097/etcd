package com.example.etcd;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class JetcdParam {

    /**
     * key
     */
    private String lock_key;

    /**
     * Lease expires time
     */
    private Long leaseTTL;

    /**
     * value
     */
    private Map<String,Object> value;

    /**
     * Whether to open the monitoring
     */
    private Boolean enWatch;

    /**
     * Lease ID
     */
    private Long leaseId;

    // getter() & setter() ...
}
