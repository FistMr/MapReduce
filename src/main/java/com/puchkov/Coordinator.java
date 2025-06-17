package com.puchkov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

class Coordinator {
    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
    private static final long TASK_TIMEOUT_MS = 30_000;
    private final int numReduceTasks;
    private final List<String> inputFiles;
    private final Map<Integer, TaskInfo> mapTasks;
    private final Map<Integer, TaskInfo> reduceTasks;
    private final AtomicInteger completedMapTasks;
    private final AtomicInteger completedReduceTasks;

    private static class TaskInfo {
        Status status;
        long startTime;

        TaskInfo(Status status) {
            this.status = status;
            this.startTime = System.currentTimeMillis();
        }

        void reset() {
            this.status = Status.AWAITING;
            this.startTime = System.currentTimeMillis();
        }
    }

    public Coordinator(List<String> inputFiles, int numReduceTasks) {

        logger.info("Инициализация Coordinator с {} файлами и {} reduce-задачами", inputFiles.size(), numReduceTasks);

        this.inputFiles = inputFiles;
        this.numReduceTasks = numReduceTasks;
        this.mapTasks = new ConcurrentHashMap<>();
        this.reduceTasks = new ConcurrentHashMap<>();
        this.completedMapTasks = new AtomicInteger(0);
        this.completedReduceTasks = new AtomicInteger(0);

        for (int i = 0; i < inputFiles.size(); i++) {
            mapTasks.put(i, new TaskInfo(Status.AWAITING));
        }

        for (int i = 0; i < numReduceTasks; i++) {
            reduceTasks.put(i, new TaskInfo(Status.AWAITING));
        }
    }

    public Task getTask() {
        logger.debug("Запрос новой задачи");
        if (completedMapTasks.get() < inputFiles.size()) {
            Task mapTask = getMapTask();
            if (mapTask != null) return mapTask;
        }

        if (completedMapTasks.get() == inputFiles.size()) {
            Task reduceTask = getReduceTask();
            if (reduceTask != null) return reduceTask;
        }

        if (isDone()) {
            cleanIntermediateFiles();
            return new Task(Task.Type.NONE, -1, null, null);
        }
        checkForStuckTasks();
        return null;
    }

    private synchronized Task getMapTask() {
        for (Map.Entry<Integer, TaskInfo> entry : mapTasks.entrySet()) {
            if (entry.getValue().status == Status.AWAITING) {
                int taskId = entry.getKey();
                entry.getValue().status = Status.IN_PROGRESS;
                String inputFile = inputFiles.get(taskId);
                logger.info("Выдана MAP-задача {} для файла {}", taskId, inputFile);
                return new Task(Task.Type.MAP, taskId, inputFile, null);
            }
        }
        return null;
    }

    private synchronized Task getReduceTask() {
        for (Map.Entry<Integer, TaskInfo> entry : reduceTasks.entrySet()) {
            if (entry.getValue().status == Status.AWAITING) {
                int taskId = entry.getKey();
                entry.getValue().status = Status.IN_PROGRESS;
                List<String> intermediateFiles = new ArrayList<>();
                for (int mapTaskId = 0; mapTaskId < inputFiles.size(); mapTaskId++) {
                    intermediateFiles.add("mr-" + mapTaskId + "-" + taskId);
                }
                logger.info("Выдана REDUCE-задача {}", taskId);
                return new Task(Task.Type.REDUCE, taskId, null, intermediateFiles);
            }
        }
        return null;
    }

    public synchronized void completeTask(Task task) {
        if (task.type == Task.Type.MAP && mapTasks.get(task.taskId).status == Status.IN_PROGRESS) {
            mapTasks.get(task.taskId).status = Status.READY;
            completedMapTasks.incrementAndGet();
            logger.info("MAP-задача {} завершена. Прогресс: {}/{}",
                    task.taskId, completedMapTasks.get(), inputFiles.size());
        } else if (task.type == Task.Type.REDUCE && reduceTasks.get(task.taskId).status == Status.IN_PROGRESS) {
            reduceTasks.get(task.taskId).status = Status.READY;
            completedReduceTasks.incrementAndGet();
            logger.info("REDUCE-задача {} завершена. Прогресс: {}/{}",
                    task.taskId, completedReduceTasks.get(), numReduceTasks);
        }
    }

    public synchronized boolean isDone() {
        return completedReduceTasks.get() == numReduceTasks;
    }

    public synchronized void reportTaskFailure(Task task) {
        if (task.type == Task.Type.MAP) {
            mapTasks.get(task.taskId).reset();
        }
        if (task.type == Task.Type.REDUCE) {
            reduceTasks.get(task.taskId).reset();
        }
    }

    private synchronized void checkForStuckTasks() {
        long now = System.currentTimeMillis();

        mapTasks.forEach((id, info) -> {
            if (info.status == Status.IN_PROGRESS &&
                    now - info.startTime > TASK_TIMEOUT_MS) {
                logger.warn("MAP-задача {} зависла, возвращаю в очередь", id);
                mapTasks.get(id).reset();
            }
        });

        reduceTasks.forEach((id, info) -> {
            if (info.status == Status.IN_PROGRESS &&
                    now - info.startTime > TASK_TIMEOUT_MS) {
                logger.warn("REDUCE-задача {} зависла, возвращаю в очередь", id);
                reduceTasks.get(id).reset();
            }
        });
    }

    private void cleanIntermediateFiles() {
        try (Stream<Path> stream = Files.walk(Paths.get("."), 1)) {
            stream.filter(path -> {
                        String name = path.getFileName().toString();
                        return name.startsWith("mr-") && !name.startsWith("mr-out-");
                    })
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                            logger.debug("Удален промежуточный файл: {}", path);
                        } catch (IOException e) {
                            logger.warn("Не удалось удалить промежуточный файл {}: {}", path, e.getMessage());
                        }
                    });
        } catch (IOException e) {
            logger.error("Ошибка очистки промежуточных файлов: {}", e.getMessage());
        }
    }
}