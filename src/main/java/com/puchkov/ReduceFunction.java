package com.puchkov;

import java.util.List;

public interface ReduceFunction {
    String reduce(String key, List<String> values);
}
