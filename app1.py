from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from src.spark_model import (
    SparkSizingInput,
    estimate_spark_resources,
    recommend_executors_for_sla,
    runtime_for_executors,
    suggest_partitions,
)


st.set_page_config(page_title="Spark Resources Calculator", page_icon="⚡", layout="wide")

st.title("⚡ Spark Resources Calculator")
st.caption(
    "Гибкая инженерная оценка времени выполнения, RAM и CPU для Spark workload. "
    "Калибруйте коэффициенты под ваш кластер и реальные метрики Spark UI."
)


with st.sidebar:
    st.header("Параметры workload")

    app_name = st.text_input("Имя приложения", value="daily_etl_job")

    st.subheader("Данные и логика")
    input_data_gb = st.number_input("Входные данные (GB, на диске)", min_value=0.1, value=500.0, step=10.0)
    compression_expand_factor = st.slider(
        "Коэффициент распаковки (compressed -> in-memory)", 1.0, 8.0, 3.0, 0.1
    )
    format_overhead_factor = st.slider(
        "Накладные расходы формата/serialization", 1.0, 2.5, 1.25, 0.05
    )
    transform_amplification_factor = st.slider(
        "Рост данных после трансформаций", 0.5, 3.0, 1.0, 0.05
    )
    shuffle_ratio = st.slider("Доля shuffle от данных", 0.0, 2.0, 0.8, 0.05)
    skew_factor = st.slider("Skew фактор", 1.0, 4.0, 1.2, 0.05)
    join_strategy = st.selectbox(
        "Тип join / паттерн workload",
        [
            "Sort-merge join",
            "Broadcast hash join",
            "Shuffle hash join",
            "Mostly aggregations (few joins)",
        ],
        index=0,
    )
    udf_complexity_factor = st.slider("Сложность UDF/логики (выше = медленнее)", 0.5, 4.0, 1.0, 0.1)

    st.subheader("Параллелизм и stages")
    stages_count = st.number_input("Количество stages", min_value=1, value=18, step=1)
    input_partitions = st.number_input("Input partitions", min_value=1, value=1200, step=10)
    shuffle_partitions = st.number_input("spark.sql.shuffle.partitions", min_value=1, value=2000, step=10)

    st.subheader("Кластер")
    executors = st.number_input("Executors", min_value=1, value=30, step=1)
    cores_per_executor = st.number_input("Cores per executor", min_value=1, value=4, step=1)
    executor_memory_gb = st.number_input("Executor memory (GB)", min_value=1.0, value=16.0, step=1.0)
    executor_memory_overhead_pct = st.slider("Executor memory overhead (%)", 5, 50, 12)
    cluster_cpu_utilization = st.slider("Эффективная утилизация CPU", 0.2, 1.0, 0.75, 0.01)

    st.subheader("Пропускная способность")
    cpu_throughput_mb_s_per_core = st.number_input(
        "CPU throughput (MB/s per core)", min_value=1.0, value=90.0, step=1.0
    )
    disk_throughput_mb_s_per_executor = st.number_input(
        "Disk throughput (MB/s per executor)", min_value=10.0, value=320.0, step=10.0
    )
    network_throughput_mb_s_per_executor = st.number_input(
        "Network throughput (MB/s per executor)", min_value=10.0, value=220.0, step=10.0
    )

    st.subheader("Memory model")
    spark_memory_fraction = st.slider("spark.memory.fraction", 0.2, 0.9, 0.6, 0.01)
    spark_storage_fraction = st.slider("spark.memory.storageFraction", 0.1, 0.9, 0.5, 0.01)
    active_working_set_fraction = st.slider("Активный working set (доля данных)", 0.05, 0.7, 0.2, 0.01)
    cache_fraction_of_data = st.slider("Кэшируемая доля данных", 0.0, 1.0, 0.2, 0.01)
    broadcast_tables_gb = st.number_input("Broadcast datasets (GB)", min_value=0.0, value=5.0, step=0.5)
    concurrent_shuffle_fraction = st.slider("Одновременный in-flight shuffle", 0.05, 0.8, 0.25, 0.01)

    st.subheader("Накладные расходы и reliability")
    stage_scheduler_overhead_s = st.number_input(
        "Overhead на stage (сек)", min_value=0.0, value=8.0, step=0.5
    )
    spill_penalty_per_100pct = st.slider("Штраф spill на каждые +100% нехватки памяти", 0.0, 2.0, 0.65, 0.05)
    gc_penalty_sensitivity = st.slider("Чувствительность к GC pressure", 0.0, 2.0, 0.9, 0.05)
    expected_retry_fraction = st.slider("Доля retried tasks", 0.0, 0.5, 0.03, 0.01)
    retry_cost_fraction = st.slider("Стоимость retry (доля runtime)", 0.0, 1.0, 0.35, 0.01)
    speculative_execution_gain = st.slider("Выгода speculative execution", 0.0, 0.35, 0.05, 0.01)

    st.subheader("SLA")
    target_runtime_min = st.number_input("Целевое время выполнения (мин, 0 = не использовать)", min_value=0.0, value=60.0, step=5.0)
    min_executors_for_search = st.number_input("Мин executors для поиска", min_value=1, value=4, step=1)
    max_executors_for_search = st.number_input("Макс executors для поиска", min_value=1, value=120, step=1)


config = SparkSizingInput(
    app_name=app_name,
    input_data_gb=float(input_data_gb),
    compression_expand_factor=float(compression_expand_factor),
    format_overhead_factor=float(format_overhead_factor),
    transform_amplification_factor=float(transform_amplification_factor),
    shuffle_ratio=float(shuffle_ratio),
    skew_factor=float(skew_factor),
    join_strategy=join_strategy,
    stages_count=int(stages_count),
    input_partitions=int(input_partitions),
    shuffle_partitions=int(shuffle_partitions),
    udf_complexity_factor=float(udf_complexity_factor),
    executors=int(executors),
    cores_per_executor=int(cores_per_executor),
    executor_memory_gb=float(executor_memory_gb),
    executor_memory_overhead_pct=float(executor_memory_overhead_pct),
    cluster_cpu_utilization=float(cluster_cpu_utilization),
    cpu_throughput_mb_s_per_core=float(cpu_throughput_mb_s_per_core),
    disk_throughput_mb_s_per_executor=float(disk_throughput_mb_s_per_executor),
    network_throughput_mb_s_per_executor=float(network_throughput_mb_s_per_executor),
    spark_memory_fraction=float(spark_memory_fraction),
    spark_storage_fraction=float(spark_storage_fraction),
    active_working_set_fraction=float(active_working_set_fraction),
    cache_fraction_of_data=float(cache_fraction_of_data),
    broadcast_tables_gb=float(broadcast_tables_gb),
    concurrent_shuffle_fraction=float(concurrent_shuffle_fraction),
    stage_scheduler_overhead_s=float(stage_scheduler_overhead_s),
    spill_penalty_per_100pct=float(spill_penalty_per_100pct),
    gc_penalty_sensitivity=float(gc_penalty_sensitivity),
    expected_retry_fraction=float(expected_retry_fraction),
    retry_cost_fraction=float(retry_cost_fraction),
    speculative_execution_gain=float(speculative_execution_gain),
    target_runtime_min=float(target_runtime_min),
    min_executors_for_search=int(min_executors_for_search),
    max_executors_for_search=int(max_executors_for_search),
)

results = estimate_spark_resources(config)
recommended_executors = recommend_executors_for_sla(config)


col1, col2, col3, col4 = st.columns(4)
col1.metric("Оценка runtime", f"{results['estimated_runtime_min']:.1f} min")
col2.metric("Требуемая память кластера", f"{results['total_memory_gb_with_overhead']:.1f} GB")
col3.metric("Всего CPU cores", f"{results['total_cores']}")
col4.metric("CPU core-hours", f"{results['cpu_core_hours']:.1f}")

col5, col6, col7, col8 = st.columns(4)
col5.metric("Execution memory", f"{results['execution_memory_gb']:.1f} GB")
col6.metric("Storage memory", f"{results['storage_memory_gb']:.1f} GB")
col7.metric("Peak execution need", f"{results['peak_execution_need_gb']:.1f} GB")
col8.metric("Spill ratio", f"{results['spill_ratio'] * 100:.1f}%")

if results["spill_ratio"] > 0.25:
    st.warning(
        "Высокий риск spill: execution memory заметно меньше пикового запроса. "
        "Увеличьте executor memory, executors или снизьте concurrent shuffle."
    )

if results["heap_pressure"] > 0.85:
    st.warning(
        "Высокое heap pressure: вероятны GC паузы и нестабильный runtime. "
        "Снизьте cache fraction, увеличьте память или переразбейте workload."
    )

if recommended_executors is None:
    st.info("SLA-автоподбор отключен (target runtime = 0).")
elif recommended_executors > config.max_executors_for_search:
    st.error("SLA недостижим в заданном диапазоне executors.")
else:
    st.success(
        f"Для SLA {config.target_runtime_min:.1f} мин: рекомендуется минимум {recommended_executors} executors."
    )

st.divider()

left, right = st.columns([1.2, 1])

with left:
    st.subheader("Breakdown времени")
    time_breakdown = pd.DataFrame(
        {
            "Компонент": ["CPU", "Disk I/O", "Network (shuffle)", "Scheduler overhead"],
            "Секунды": [
                results["cpu_seconds"],
                results["disk_seconds"],
                results["network_seconds"],
                results["scheduling_overhead_seconds"],
            ],
        }
    )
    fig_time = px.bar(
        time_breakdown,
        x="Компонент",
        y="Секунды",
        title="Оценка bottleneck по времени",
        text_auto=".1f",
    )
    st.plotly_chart(fig_time, use_container_width=True)

with right:
    st.subheader("Breakdown памяти")
    memory_breakdown = pd.DataFrame(
        {
            "Компонент": [
                "Execution available",
                "Storage available",
                "Working set",
                "Shuffle in-flight",
                "Cache need",
                "Broadcast",
            ],
            "GB": [
                results["execution_memory_gb"],
                results["storage_memory_gb"],
                results["working_set_gb"],
                results["shuffle_in_flight_gb"],
                results["cache_need_gb"],
                config.broadcast_tables_gb,
            ],
        }
    )
    fig_memory = px.bar(memory_breakdown, x="Компонент", y="GB", text_auto=".1f")
    st.plotly_chart(fig_memory, use_container_width=True)

st.subheader("Чувствительность runtime к числу executors")
search_from = max(1, min(config.min_executors_for_search, config.max_executors_for_search))
search_to = max(search_from, config.max_executors_for_search)

curve_data = pd.DataFrame(
    {
        "Executors": list(range(search_from, search_to + 1)),
    }
)
curve_data["Runtime (min)"] = curve_data["Executors"].apply(
    lambda count: runtime_for_executors(config, int(count))
)
fig_curve = px.line(curve_data, x="Executors", y="Runtime (min)", markers=True)
fig_curve.update_layout(xaxis_title="Executors", yaxis_title="Runtime (min)")
st.plotly_chart(fig_curve, use_container_width=True)

st.subheader("Рекомендации по конфигурации")
recommended_shuffle = suggest_partitions(results["total_cores"])

recommendations = pd.DataFrame(
    {
        "Параметр": [
            "Recommended executor memory (GB, no-spill target)",
            "Recommended shuffle partitions",
            "Current shuffle partitions",
            "Storage pressure",
            "Wave factor",
        ],
        "Value": [
            f"{results['recommended_executor_memory_gb']:.1f}",
            f"{recommended_shuffle}",
            f"{config.shuffle_partitions}",
            f"{results['storage_pressure_ratio'] * 100:.1f}%",
            f"{results['waves']:.2f}",
        ],
    }
)
st.dataframe(recommendations, use_container_width=True, hide_index=True)

st.caption(
    "Модель учитывает основные факторы производительности Spark, но точность зависит от калибровки "
    "throughput/penalty коэффициентов под ваш конкретный кластер и тип workload."
)
