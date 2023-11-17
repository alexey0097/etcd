package com.example.etcd;

import io.etcd.jetcd.KeyValue;
import lombok.Getter;
import lombok.Setter;

import java.util.Collection;
import java.util.List;

@Getter
@Setter
public class JetcdResp {

    private String desc;
    private Object data;

}
