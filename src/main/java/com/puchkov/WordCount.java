package com.puchkov;

import java.util.ArrayList;
import java.util.List;

public class WordCount {
    public static class Mapper implements MapFunction {
        @Override
        public List<KeyValue> map(String fileName, String content) {
            String[] words = content.split("\\s+");
            List<KeyValue> kvs = new ArrayList<>();
            for (String word : words) {
                kvs.add(new KeyValue(word, "1"));
            }
            return kvs;
        }
    }

    public static class Reducer implements ReduceFunction {
        @Override
        public String reduce(String key, List<String> values) {
            return String.valueOf(values.size());
        }
    }
}