package com.puchkov;

import java.util.List;

public class Task {
    enum Type { MAP, REDUCE, NONE }

    Type type;
    int taskId;
    String inputFile;
    List<String> intermediateFiles;

    public Task(Type type, int taskId, String inputFile, List<String> intermediateFiles) {
        this.type = type;
        this.taskId = taskId;
        this.inputFile = inputFile;
        this.intermediateFiles = intermediateFiles;
    }
}
