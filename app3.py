import streamlit as st
import math

# --- Настройка страницы ---
st.set_page_config(page_title="Spark Resource Calculator", layout="wide", page_icon="⚡")

st.title("⚡ Профессиональный калькулятор ресурсов Apache Spark")
st.markdown("""
Этот инструмент рассчитывает оптимальную конфигурацию `spark-submit` для вашего кластера на основе лучших практик индустрии (включая лимиты сжатия указателей, I/O узкие места и накладные расходы JVM).
*Режим: Scala/Java (Без PySpark), активное использование памяти.*
""")

# --- Блок 1: Входные параметры ---
st.header("🎛 1. Входные параметры")
col1, col2 = st.columns(2)

with col1:
    st.subheader("Основные настройки (Basic)")
    data_size_gb = st.number_input("Объем сырых входных данных (ГБ)", min_value=1, value=1000, step=100, 
                                   help="Объем данных на диске до загрузки в Spark")
    data_format = st.selectbox("Формат входных данных", ["Parquet / ORC", "Сжатый CSV / JSON", "Обычный текст"], 
                               help="Определяет коэффициент раздувания (inflation factor) данных при десериализации в память Java.")
    node_cores = st.number_input("Количество ядер на 1 узле (vCPU)", min_value=2, value=16)
    node_ram = st.number_input("Объем памяти на 1 узле (ГБ)", min_value=4, value=64)
    num_nodes = st.number_input("Количество доступных узлов (Nodes) в кластере", min_value=2, value=6)

with col2:
    st.subheader("⚙️ Продвинутые настройки (Advanced)")
    target_exec_cores = st.number_input("Целевое кол-во ядер на экзекьютор", min_value=1, max_value=node_cores, value=5, 
                                        help="Рекомендуется 5 ядер для баланса HDFS I/O и Garbage Collection.")
    os_cores = st.number_input("Резерв ядер для ОС и демонов", min_value=0, value=1)
    os_ram = st.number_input("Резерв памяти для ОС и демонов (ГБ)", min_value=0, value=1)
    partition_size_mb = st.number_input("Целевой размер Shuffle Partition (МБ)", min_value=10, value=128, 
                                        help="Идеальный размер блока данных на 1 таску (от 100 до 200 МБ).")
    memory_fraction = st.slider("spark.memory.fraction", min_value=0.1, max_value=0.9, value=0.6, step=0.1,
                                help="Доля памяти JVM для Execution (shuffle) и Storage (cache). Оставшееся уходит под User Memory.")

# --- Блок 2: Математическое ядро (Математика калькулятора) ---
# 1. Расчет ядер
available_cores_per_node = node_cores - os_cores
executors_per_node = available_cores_per_node // target_exec_cores

if executors_per_node < 1:
    st.error("🚨 **Критическая ошибка:** Недостаточно ядер на узле для выделения хотя бы одного экзекьютора!")
    st.stop()

# 1 экзекьютор всегда забирается под Driver (Application Master) в режиме cluster
total_executors = (executors_per_node * num_nodes) - 1 

if total_executors < 1:
    st.error("🚨 **Критическая ошибка:** Слишком мало ресурсов для запуска (нужен минимум 1 Driver и 1 Executor). Увеличьте кластер.")
    st.stop()

total_compute_cores = total_executors * target_exec_cores

# 2. Расчет памяти
available_ram_per_node = node_ram - os_ram
raw_mem_per_exec = available_ram_per_node / executors_per_node

# Overhead для Scala/Java (10% от памяти экзекьютора, минимум 384 МБ)
overhead_mem_gb = max(0.384, raw_mem_per_exec * 0.10)
heap_mem_gb = raw_mem_per_exec - overhead_mem_gb

# 3. Расчет Shuffle Partitions
inflation_map = {"Parquet / ORC": 4, "Сжатый CSV / JSON": 8, "Обычный текст": 3}
inflation_factor = inflation_map[data_format]
data_in_memory_mb = data_size_gb * inflation_factor * 1024

# Базовое количество партиций = Размер раздутых данных / целевой размер партиции
base_partitions = math.ceil(data_in_memory_mb / partition_size_mb)
# Чтобы не было простаивающих ядер (idle cores), округляем вверх кратно общему количеству ядер
partition_multiplier = math.ceil(base_partitions / total_compute_cores)
shuffle_partitions = max(200, partition_multiplier * total_compute_cores)

# Полезная память кластера для данных (Execution + Storage)
# Учитываем Reserved Memory (300 MB)
unified_mem_per_exec_gb = (heap_mem_gb - 0.3) * memory_fraction
cluster_unified_mem_gb = unified_mem_per_exec_gb * total_executors

# --- Блок 3: Вывод результатов ---
st.markdown("---")
st.header("✅ 2. Рекомендованная конфигурация")

# Визуальные метрики
m1, m2, m3, m4 = st.columns(4)
m1.metric("Экзекьюторов (Instances)", total_executors)
m2.metric("Ядер на Экзекьютор (Cores)", target_exec_cores)
m3.metric("Память Экзекьютора (Heap)", f"{math.floor(heap_mem_gb)} GB")
m4.metric("Shuffle Partitions", shuffle_partitions)

st.code(f"""
# Флаги для spark-submit
--conf spark.executor.instances={total_executors}
--conf spark.executor.cores={target_exec_cores}
--conf spark.executor.memory={math.floor(heap_mem_gb)}g
--conf spark.executor.memoryOverhead={math.ceil(overhead_mem_gb * 1024)}m
--conf spark.sql.shuffle.partitions={shuffle_partitions}
--conf spark.default.parallelism={shuffle_partitions}
--conf spark.memory.fraction={memory_fraction}
--conf spark.sql.adaptive.enabled=true
""", language="bash")

# --- Блок 4: Человекочитаемое описание ---
st.subheader("💡 Почему выбраны эти параметры?")
st.write(f"""
1. **Топология кластера:** Мы зарезервировали {os_cores} ядро и {os_ram} ГБ памяти на каждом узле под операционную систему и YARN NodeManager. Оставшиеся ресурсы узла мы разделили на **{executors_per_node} экзекьютора(-ов)** по {target_exec_cores} ядер. Один из процессов в кластере выделен под координацию (Spark Driver).
2. **Распределение памяти:** Каждому процессу Spark выделяется **{math.floor(heap_mem_gb)} ГБ** JVM Heap (кучи) и **{math.ceil(overhead_mem_gb * 1024)} МБ** Off-Heap (Memory Overhead) для сетевых буферов. Мы не используем PySpark, поэтому стандартных 10% overhead вполне достаточно.
3. **Параллелизм:** Из-за структуры формата `{data_format}` ваши {data_size_gb} ГБ на диске распакуются примерно в **{data_size_gb * inflation_factor} ГБ** объектов в оперативной памяти Java. Мы рассчитали количество `shuffle.partitions` равным **{shuffle_partitions}**, чтобы каждая задача обрабатывала ровно ~{partition_size_mb} МБ данных. Кроме того, это число кратно общему пулу в {total_compute_cores} ядер кластера, чтобы в последней волне тасок не было простаивающих ресурсов.
""")

# --- Блок 5: Модуль анализа рисков (Alerts & Warnings) ---
st.markdown("---")
st.header("🚨 3. Анализ рисков и предупреждения")

has_warnings = False

# Риск 1: Лимит 32 ГБ (Compressed OOPs)
if heap_mem_gb > 32:
    has_warnings = True
    st.warning(f"**Риск отключения Compressed OOPs:** Размер Heap ({math.floor(heap_mem_gb)} ГБ) превышает 32 ГБ. "
               f"В этом случае JVM начинает использовать 64-битные указатели вместо 32-битных, что приведет к увеличению потребления памяти всеми объектами на 20-30% и нагрузке на сборщик мусора (GC). "
               f"*Решение:* Уменьшите число ядер на узел (выделив больше экзекьюторов) или оставьте часть памяти нераспределенной.")

# Риск 2: Fat Executors (I/O Bottleneck)
if target_exec_cores > 5:
    has_warnings = True
    st.warning(f"**Узкое место HDFS / Network I/O:** Использование более 5 потоков на один процесс ({target_exec_cores}) часто приводит к деградации пропускной способности сети и HDFS (Hadoop Distributed File System). "
               f"Также это сильно увеличит паузы сборки мусора (Stop-The-World GC). "
               f"*Решение:* Верните 'Целевое кол-во ядер' на уровень 4-5.")

# Риск 3: Tiny Executors
if target_exec_cores == 1:
    has_warnings = True
    st.warning(f"**Слишком маленькие экзекьюторы:** Выделение 1 ядра на экзекьютор убивает смысл многопоточности в одной JVM. "
               f"Переменные Broadcast будут дублироваться для каждого ядра на узле отдельно, что катастрофически забьет память. "
               f"*Решение:* Увеличьте 'Целевое кол-во ядер' до 3-5.")

# Риск 4: Spill на диск (Нехватка Unified Memory)
if (data_size_gb * inflation_factor) > cluster_unified_mem_gb:
    has_warnings = True
    st.error(f"**Высокий риск дисковых спиллов (Disk Spills) и OOM:** Совокупный объем памяти кластера, доступной под исполнение и кэш (Unified Memory ≈ {math.floor(cluster_unified_mem_gb)} ГБ), "
             f"МЕНЬШЕ, чем ожидаемый объем раздутых данных в памяти (≈ {data_size_gb * inflation_factor} ГБ). "
             f"Spark будет сбрасывать промежуточные расчеты и Shuffle на локальные диски, что замедлит работу в несколько раз. "
             f"*Решение:* Выделите больше узлов, используйте сериализацию `.persist(StorageLevel.MEMORY_AND_DISK_SER)` или уменьшите `spark.memory.fraction`, если вы не кэшируете данные `df.cache()`.")
elif (data_size_gb * inflation_factor) > (cluster_unified_mem_gb * 0.7):
    has_warnings = True
    st.info(f"**Заметная утилизация памяти:** Полезная память кластера заполнена более чем на 70%. Поскольку вы активно используете память, убедитесь, что применяете `KryoSerializer` (повышает скорость сериализации) и агрессивное освобождение кэша (`unpersist()`), когда данные больше не нужны.")

if not has_warnings:
    st.success("Отлично! При текущих настройках конфликтов архитектуры или явных рисков (Spills / OOM / GC Pauses) не обнаружено. Конфигурация выглядит сбалансированной.")

# Дополнительные советы
with st.expander("🛠 Профессиональные советы для In-Memory обработки"):
    st.markdown("""
    * **Используйте Kryo Serialization:** Задайте `--conf spark.serializer=org.apache.spark.serializer.KryoSerializer`. Kryo сжимает объекты в памяти гораздо агрессивнее, чем встроенная сериализация Java.
    * **Управление кэшем:** Так как вы активно используете память, не забывайте про `spark.memory.storageFraction`. Если джоб много кэширует, можно поднять `spark.memory.fraction` до 0.7-0.8.
    * **Сборка мусора (GC):** Если Heap превышает 8GB, обязательно включайте G1GC Collector: `--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35"`.
    """)