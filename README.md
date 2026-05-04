# MapReduce

Учебная реализация фреймворка **MapReduce** на Java, вдохновлённая классической статьёй Google и лабораторной работой MIT 6.5840. Поддерживает параллельную обработку входных файлов несколькими воркерами в рамках одного процесса (in-process), управление задачами через `Coordinator`, переотправку «зависших» задач и пользовательские `map`/`reduce`-функции.

---

## 📖 Описание

Проект предоставляет минимальный, но рабочий MapReduce-движок:

- **Coordinator** распределяет задачи между воркерами, отслеживает их статус (`AWAITING` / `IN_PROGRESS` / `READY`) и автоматически переотправляет зависшие задачи (таймаут 30 с).
- **Worker** запускается в пуле потоков, опрашивает координатор, выполняет фазы **map** или **reduce**, читает входные данные и пишет промежуточные/итоговые файлы.
- **MapFunction / ReduceFunction** — пользовательские интерфейсы, через которые подключается бизнес-логика (см. встроенный пример **WordCount**).
- Промежуточные результаты партиционируются по `hashCode(key) % numReduceTasks` и сериализуются стандартным `ObjectOutputStream`.
- По завершении всех задач промежуточные файлы (`mr-*`, кроме `mr-out-*`) автоматически удаляются.

**Возможности:**
- Параллельный запуск произвольного числа воркеров.
- Произвольное число reduce-партиций.
- Подключаемые `Mapper`/`Reducer`.
- Автоматический повтор «упавших» / зависших задач.
- Логирование через SLF4J + Logback.

---

## 🛠️ Технологический стек

| Компонент | Версия |
|---|---|
| Язык | Java |
| Сборка | Apache Maven |
| Логирование | SLF4J `2.0.17` + Logback Classic `1.5.18` |
| Тесты | JUnit `3.8.1` |
| Параллелизм | `java.util.concurrent` (ExecutorService, ConcurrentHashMap, AtomicInteger) |
| Сериализация | Стандартная Java Serialization (`ObjectOutputStream`) |

> ⚠️ **TODO:** в `pom.xml` не указаны `maven-compiler-plugin` и целевая версия Java (`maven.compiler.source/target`). Рекомендуется явно зафиксировать версию (например, Java 17) и обновить JUnit до 4.x/5.x.

---

## 📂 Структура проекта

```
MapReduce/
├── pom.xml                              # Maven-конфигурация и зависимости
├── file1.txt, file2.txt                 # Примеры входных файлов
├── mr-out-0                             # Пример итогового вывода reduce-задачи
├── src/
│   ├── main/java/com/puchkov/
│   │   ├── MapReduce.java               # Точка входа (main), запуск пула воркеров
│   │   ├── Coordinator.java             # Координатор задач, хранит состояние, контролирует таймауты
│   │   ├── Worker.java                  # Воркер: выполняет map/reduce, пишет файлы
│   │   ├── Task.java                    # Описание задачи (Type: MAP/REDUCE/NONE)
│   │   ├── Status.java                  # Статус задачи: AWAITING / IN_PROGRESS / READY
│   │   ├── KeyValue.java                # Сериализуемая пара ключ–значение
│   │   ├── MapFunction.java             # Интерфейс map-функции
│   │   ├── ReduceFunction.java          # Интерфейс reduce-функции
│   │   └── WordCount.java               # Встроенный пример (Mapper + Reducer)
│   └── test/java/com/puchkov/
│       └── AppTest.java                 # Заглушка JUnit 3 (TODO: написать реальные тесты)
└── .gitignore
```

---

## ✅ Требования

- **JDK** 8+ (рекомендуется 11 или 17). Конкретная версия в `pom.xml` не зафиксирована — **TODO**.
- **Apache Maven** 3.6+.
- ОС: любая, поддерживающая JVM (проект разрабатывался на Windows 11, путь к рабочей директории определяется JVM).
- Внешние сервисы / БД: **не требуются**.

---

## ⚙️ Установка

```bash
# 1. Клонировать репозиторий
git clone <repository-url>
cd MapReduce

# 2. Собрать проект (скачивает зависимости, компилирует и прогоняет тесты)
mvn clean package
```

После сборки скомпилированные классы окажутся в `target/classes`, а JAR — в `target/MapReduce-1.0-SNAPSHOT.jar`.

---

## 🔧 Конфигурация

Проект **не использует** файлы `.env`, `application.properties` или внешние конфиги — все параметры передаются как **аргументы командной строки**.

| Параметр | Тип | Описание |
|---|---|---|
| `inputFiles...` | `String[]` | Один или несколько путей к входным текстовым файлам |
| `numWorkers` | `int` | Размер пула воркеров (предпоследний аргумент) |
| `numReduceTasks` | `int` | Количество reduce-партиций (последний аргумент) |

**Внутренние константы** (заданы в коде):

| Константа | Значение | Где |
|---|---|---|
| `TASK_TIMEOUT_MS` | `30_000` мс | `Coordinator.java`, `Worker.java` — таймаут зависшей задачи и таймаут ожидания новых задач воркером |

> **TODO:** вынести таймауты и параметры пула в конфигурационный файл / системные свойства.

---

## ▶️ Запуск

### Режим разработки (через Maven exec-plugin)

В текущем `pom.xml` `exec-maven-plugin` **не подключён** (TODO), поэтому используйте прямой запуск через `java -cp`.

### Запуск встроенного примера WordCount

```bash
# 1. Собрать
mvn clean package

# 2. Запустить main-класс
java -cp target/classes;target/dependency/* com.puchkov.MapReduce file1.txt file2.txt 4 3
#                  ^ путь-разделитель ";" для Windows, ":" для Linux/macOS
#                                                       ^файлы^   ^воркеры^ ^reduce^
```

Если зависимости не лежат в `target/dependency/`, используйте плагин:

```bash
mvn dependency:copy-dependencies
```

Либо запускайте через сам Maven:

```bash
mvn compile exec:java -Dexec.mainClass="com.puchkov.MapReduce" \
                      -Dexec.args="file1.txt file2.txt 4 3"
```
> Для этого добавьте `exec-maven-plugin` в `pom.xml` (**TODO**).

### Production-режим

Сборка в jar-with-dependencies через `maven-shade-plugin` или `maven-assembly-plugin` **не настроена** — **TODO**.
После настройки запуск будет:

```bash
java -jar target/MapReduce-1.0-SNAPSHOT.jar file1.txt file2.txt 4 3
```

### Docker

Dockerfile / docker-compose.yml в проекте **отсутствуют** — **TODO**.

---

## 📡 API Документация

Проект **не предоставляет HTTP/REST/gRPC API** — это библиотека/CLI-приложение. Ниже описаны два публичных интерфейса: **CLI** и **программный API**.

### 1. CLI

#### `MapReduce` — запуск задания

| Параметр | Позиция | Тип | Обязательный | Описание |
|---|---|---|---|---|
| `inputFiles` | `1..N-2` | `String...` | да | Пути к входным файлам |
| `numWorkers` | `N-1` | `int` | да | Количество воркеров |
| `numReduceTasks` | `N` | `int` | да | Количество reduce-задач |

**Пример вызова:**

```bash
java -cp target/classes com.puchkov.MapReduce file1.txt file2.txt 4 3
```

**Артефакты на выходе:**
- `mr-out-0`, `mr-out-1`, …, `mr-out-{numReduceTasks-1}` — итоговые файлы. Каждая строка имеет формат:
  ```
  <key> <result>
  ```
  Пример (`mr-out-0` встроенного WordCount):
  ```
  csharp 8
  dart 5
  go 9
  java 8
  ...
  ```
- Промежуточные файлы `mr-<mapTaskId>-<reduceTaskId>` создаются на этапе Map и удаляются после завершения всех reduce-задач.

**Коды возврата:** стандартные коды JVM. Ошибки логируются через SLF4J. **TODO:** ввести явные exit-коды (например, `1` — неверные аргументы, `2` — IO-ошибка).

---

### 2. Программный API

#### `com.puchkov.MapReduce#run(...)`

Главный метод-оркестратор.

```java
public static void run(
    List<String>   inputFiles,
    int            numWorkers,
    int            numReduceTasks,
    MapFunction    mapFunc,
    ReduceFunction reduceFunc
)
```

| Аргумент | Тип | Описание |
|---|---|---|
| `inputFiles` | `List<String>` | Пути к входным файлам |
| `numWorkers` | `int` | Размер пула потоков-воркеров |
| `numReduceTasks` | `int` | Количество reduce-партиций |
| `mapFunc` | `MapFunction` | Пользовательская реализация фазы Map |
| `reduceFunc` | `ReduceFunction` | Пользовательская реализация фазы Reduce |

#### `MapFunction`

```java
public interface MapFunction {
    List<KeyValue> map(String fileName, String content);
}
```

| Параметр | Тип | Описание |
|---|---|---|
| `fileName` | `String` | Имя обрабатываемого файла |
| `content` | `String` | Полное содержимое файла |
| **возврат** | `List<KeyValue>` | Список промежуточных пар ключ–значение |

#### `ReduceFunction`

```java
public interface ReduceFunction {
    String reduce(String key, List<String> values);
}
```

| Параметр | Тип | Описание |
|---|---|---|
| `key` | `String` | Ключ |
| `values` | `List<String>` | Все значения, ассоциированные с ключом |
| **возврат** | `String` | Итоговое значение, записываемое в `mr-out-*` |

#### Пример: WordCount

```java
import com.puchkov.MapReduce;
import com.puchkov.WordCount;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        MapReduce.run(
            List.of("file1.txt", "file2.txt"),
            4,                          // numWorkers
            3,                          // numReduceTasks
            new WordCount.Mapper(),
            new WordCount.Reducer()
        );
    }
}
```

#### Жизненный цикл задачи (`Task.Type`)

| Тип | Описание |
|---|---|
| `MAP` | Обработка одного входного файла, генерация промежуточных пар KV |
| `REDUCE` | Сборка одной партиции из промежуточных файлов и запись результата |
| `NONE` | Сигнал воркеру о завершении работы |

#### Возможные ошибки

| Условие | Поведение |
|---|---|
| `args.length < 3` в `main` | Лог `error`, выход без обработки |
| Зависшая задача (`> 30 c` в `IN_PROGRESS`) | Сбрасывается в `AWAITING` и переотправляется |
| `IOException` при чтении/записи файла | Лог `error`, `coordinator.reportTaskFailure(task)` — задача переотправляется |
| `ClassNotFoundException` при десериализации промежуточного файла | Лог `error`, частичные данные могут быть потеряны (**TODO**: жёстче обрабатывать) |
| Воркер не получал задач `> 30 c` | Лог `error`, поток завершается |

---

## 📋 Сводная таблица TODO

| Область | Задача |
|---|---|
| Сборка | Зафиксировать `maven.compiler.source/target`, добавить `maven-compiler-plugin` |
| Сборка | Подключить `maven-shade-plugin` для fat-jar |
| Сборка | Подключить `exec-maven-plugin` для `mvn exec:java` |
| Тесты | Мигрировать с JUnit 3 на JUnit 5, написать реальные тесты, добавить Jacoco |
| Конфигурация | Вынести таймауты и параметры в конфиг/системные свойства |
| Документация | Добавить Javadoc к публичным классам |
| Надёжность | Заменить Java Serialization на устойчивый формат (JSON/Avro) |
| Логирование | Добавить `logback.xml` с настройками уровней по пакетам |
