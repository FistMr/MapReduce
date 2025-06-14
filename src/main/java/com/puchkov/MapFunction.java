package com.puchkov;

import java.util.List;

public interface MapFunction {
    List<KeyValue> map(String fileName, String content);
}
