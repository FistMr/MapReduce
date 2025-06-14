package com.puchkov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

class Coordinator {
    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
    private final int numReduceTasks;
    private final List<String> inputFiles;
    private final Map<Integer, Boolean> mapTasks;
    private final Map<Integer, Boolean> reduceTasks;
    private final AtomicInteger completedMapTasks;
    private final AtomicInteger completedReduceTasks;

    public Coordinator(List<String> inputFiles, int numReduceTasks) {

        logger.info("Инициализация Coordinator с {} файлами и {} reduce-задачами", inputFiles.size(), numReduceTasks);

        this.inputFiles = inputFiles;
        this.numReduceTasks = numReduceTasks;
        this.mapTasks = new ConcurrentHashMap<>();
        this.reduceTasks = new ConcurrentHashMap<>();
        this.completedMapTasks = new AtomicInteger(0);
        this.completedReduceTasks = new AtomicInteger(0);

        for (int i = 0; i < inputFiles.size(); i++) {
            mapTasks.put(i, false);
        }

        for (int i = 0; i < numReduceTasks; i++) {
            reduceTasks.put(i, false);
        }
    }

    public synchronized Task getTask() {

        logger.debug("Запрос новой задачи");

        for (Map.Entry<Integer, Boolean> entry : mapTasks.entrySet()) {
            if (!entry.getValue()) {
                int taskId = entry.getKey();
                String inputFile = inputFiles.get(taskId);
                logger.info("Выдана MAP-задача {} для файла {}", taskId, inputFile);
                return new Task(Task.Type.MAP, taskId, inputFile, null);
            }
        }

        if (completedMapTasks.get() == inputFiles.size()) {
            logger.debug("Все MAP-задачи завершены, проверка REDUCE-задач");
            for (Map.Entry<Integer, Boolean> entry : reduceTasks.entrySet()) {
                if (!entry.getValue()) {
                    int taskId = entry.getKey();
                    List<String> intermediateFiles = new ArrayList<>();
                    for (int mapTaskId = 0; mapTaskId < inputFiles.size(); mapTaskId++) {
                        intermediateFiles.add("mr-" + mapTaskId + "-" + taskId);
                    }
                    logger.info("Выдана REDUCE-задача {}", taskId);
                    return new Task(Task.Type.REDUCE, taskId, null, intermediateFiles);
                }
            }
        }

        if (completedReduceTasks.get() == numReduceTasks) {
            logger.info("Все задачи выполнены");
            return new Task(Task.Type.NONE, -1, null, null);
        }

        logger.debug("Нет доступных задач на данный момент");
        return null;
    }

    public synchronized void completeTask(Task task) {
        if (task.type == Task.Type.MAP && !mapTasks.get(task.taskId)) {
            mapTasks.put(task.taskId, true);
            completedMapTasks.incrementAndGet();
            logger.info("MAP-задача {} завершена. Прогресс: {}/{}",
                    task.taskId, completedMapTasks.get(), inputFiles.size());
        } else if (task.type == Task.Type.REDUCE && !reduceTasks.get(task.taskId)) {
            reduceTasks.put(task.taskId, true);
            completedReduceTasks.incrementAndGet();
            logger.info("REDUCE-задача {} завершена. Прогресс: {}/{}",
                    task.taskId, completedReduceTasks.get(), numReduceTasks);
        }
    }

    public synchronized boolean isDone() {
        boolean done = completedReduceTasks.get() == numReduceTasks;
        if (done) {
            logger.info("Все задачи успешно выполнены!");
        }
        return done;
    }
}
