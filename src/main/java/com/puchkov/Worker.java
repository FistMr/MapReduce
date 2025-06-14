package com.puchkov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Worker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);
    private final Coordinator coordinator;
    private final MapFunction mapFunc;
    private final ReduceFunction reduceFunc;
    private final int numReduceTasks;

    public Worker(Coordinator coordinator, MapFunction mapFunc, ReduceFunction reduceFunc, int numReduceTasks) {
        this.coordinator = coordinator;
        this.mapFunc = mapFunc;
        this.reduceFunc = reduceFunc;
        this.numReduceTasks = numReduceTasks;
    }

    @Override
    public void run() {
        while (true) {
            Task task = coordinator.getTask();

            if (task == null) {
                try {
                    logger.debug("Нет доступных задач, ожидание");
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                continue;
            }

            if (task.type == Task.Type.NONE) {
                logger.info("Получена команда на завершение работы");
                return;
            }

            if (task.type == Task.Type.MAP) {
                try {
                    String content = new String(Files.readAllBytes(Paths.get(task.inputFile)));

                    List<KeyValue> kvs = mapFunc.map(task.inputFile, content);

                    Map<Integer, List<KeyValue>> buckets = new HashMap<>();
                    for (KeyValue kv : kvs) {
                        int bucket = Math.abs(kv.key.hashCode()) % numReduceTasks;
                        if (!buckets.containsKey(bucket)) {
                            buckets.put(bucket, new ArrayList<>());
                        }
                        buckets.get(bucket).add(kv);
                    }

                    for (int bucket = 0; bucket < numReduceTasks; bucket++) {
                        String filename = "mr-" + task.taskId + "-" + bucket;
                        List<KeyValue> data = buckets.getOrDefault(bucket, new ArrayList<>());

                        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename))) {
                            out.writeObject(data);
                        }
                    }

                    coordinator.completeTask(task);
                } catch (IOException e) {
                    logger.error("Ошибка обработки MAP-задачи {}: {}", task.taskId, e.getMessage());
                }
            } else if (task.type == Task.Type.REDUCE) {
                try {
                    List<KeyValue> allKVs = new ArrayList<>();
                    for (String filename : task.intermediateFiles) {
                        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(filename))) {
                            List<KeyValue> kvs = (List<KeyValue>) in.readObject();
                            allKVs.addAll(kvs);
                        } catch (ClassNotFoundException e) {
                            logger.error("Ошибка десериализации из файла: {}", filename, e);
                        }
                    }

                    allKVs.sort(Comparator.comparing(kv -> kv.key));

                    Map<String, List<String>> grouped = new LinkedHashMap<>();
                    for (KeyValue kv : allKVs) {
                        if (!grouped.containsKey(kv.key)) {
                            grouped.put(kv.key, new ArrayList<>());
                        }
                        grouped.get(kv.key).add(kv.value);
                    }

                    String outputFile = "mr-out-" + task.taskId;
                    try (PrintWriter writer = new PrintWriter(new FileWriter(outputFile))) {
                        for (Map.Entry<String, List<String>> entry : grouped.entrySet()) {
                            String result = reduceFunc.reduce(entry.getKey(), entry.getValue());
                            writer.println(entry.getKey() + " " + result);
                        }
                    }

                    coordinator.completeTask(task);
                } catch (IOException e) {
                    logger.error("Ошибка обработки REDUCE-задачи {}: {}", task.taskId, e.getMessage());
                }
            }
        }
    }
}
