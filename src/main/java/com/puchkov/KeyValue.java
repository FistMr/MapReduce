package com.puchkov;

import java.io.Serializable;

public class KeyValue implements Serializable {
    String key;
    String value;

    public KeyValue(String key, String value) {
        this.key = key;
        this.value = value;
    }

}
