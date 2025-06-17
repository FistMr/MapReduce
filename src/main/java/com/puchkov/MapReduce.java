package com.puchkov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MapReduce {
    private static final Logger logger = LoggerFactory.getLogger(MapReduce.class);

    public static void main(String[] args) {
        if (args.length < 3) {
            logger.error("Неверные аргументы. Используйте шаблон: MapReduce <inputFiles...> <numWorkers> <numReduceTasks>");
            return;
        }

        List<String> inputFiles = new ArrayList<>();
        int numWorkers = Integer.parseInt(args[args.length - 2]);
        int numReduceTasks = Integer.parseInt(args[args.length - 1]);

        for (int i = 0; i < args.length - 2; i++) {
            inputFiles.add(args[i]);
        }

        logger.info("Запуск MapReduce с параметрами:");
        logger.info("Файлы: {}", inputFiles);
        logger.info("Количество воркеров: {}", numWorkers);
        logger.info("Количество reduce-задач: {}", numReduceTasks);
        MapReduce.run(inputFiles, numWorkers, numReduceTasks, new WordCount.Mapper(), new WordCount.Reducer());
    }

    public static void run(List<String> inputFiles, int numWorkers, int numReduceTasks,
                           MapFunction mapFunc, ReduceFunction reduceFunc) {
        Coordinator coordinator = new Coordinator(inputFiles, numReduceTasks);

        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
        for (int i = 0; i < numWorkers; i++) {
            executor.submit(new Worker(coordinator, mapFunc, reduceFunc, numReduceTasks));
        }

        while (!coordinator.isDone()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.warn("Главный поток был прерван, экстренное завершение");
                executor.shutdownNow();
                return;
            }
        }
        logger.info("Все задачи успешно выполнены!");

        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                logger.warn("Воркеры не завершились в отведенное время, принудительная остановка");

                List<Runnable> unfinishedTasks = executor.shutdownNow();
                logger.warn("Принудительно остановлено {} воркеров", unfinishedTasks.size());
            }
        } catch (InterruptedException e) {
            logger.warn("Завершение работы было прервано", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("Задание MapReduce успешно завершено");
    }
}
