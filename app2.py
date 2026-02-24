from __future__ import annotations

from dataclasses import dataclass
from math import ceil
from typing import Dict, Tuple

import streamlit as st

GB_IN_MB = 1024
MB_IN_BYTES = 1024 * 1024


@dataclass(frozen=True)
class CalcInput:
    data_volume_gb: float
    data_format: str
    complexity: str
    sla_minutes: int
    use_cache: bool
    shuffle_intensity: str
    skew_risk: bool
    safety_buffer_pct: int
    max_executors_cap: int


@dataclass(frozen=True)
class CalcResult:
    executors: int
    executor_cores: int
    executor_memory_mb: int
    executor_overhead_mb: int
    driver_memory_mb: int
    driver_cores: int
    total_cluster_cores: int
    total_cluster_ram_mb: int
    effective_data_mb: int
    aqe_partition_mb: int
    total_partitions: int
    required_parallel_tasks: int
    shuffle_partitions: int
    advisory_partition_bytes: int
    rationale: str


FORMAT_IN_MEMORY_FACTOR: Dict[str, float] = {
    "Parquet": 1.20,
    "Iceberg": 1.15,
    "ORC": 1.25,
    "CSV": 2.80,
}

COMPLEXITY_TASK_MEMORY_MB: Dict[str, int] = {
    "Low": 384,
    "Medium": 768,
    "High": 1536,
}

# Approximate core throughput in MB/sec for Spark SQL ETL (Scala/JVM), by complexity.
COMPLEXITY_THROUGHPUT_MBPS: Dict[str, float] = {
    "Low": 75.0,
    "Medium": 45.0,
    "High": 24.0,
}

SHUFFLE_MULTIPLIER: Dict[str, float] = {
    "Standard": 1.00,
    "Heavy": 1.25,
    "Extreme": 1.45,
}

PARTITIONS_PER_CORE_TARGET: Dict[str, int] = {
    "Low": 24,
    "Medium": 16,
    "High": 10,
}

PROFILE_DEFAULTS: Dict[str, Dict[str, int | str | bool]] = {
    "Conservative": {
        "shuffle_intensity": "Heavy",
        "skew_risk": True,
        "safety_buffer_pct": 25,
        "max_executors_cap": 180,
    },
    "Balanced": {
        "shuffle_intensity": "Standard",
        "skew_risk": False,
        "safety_buffer_pct": 10,
        "max_executors_cap": 250,
    },
    "Aggressive": {
        "shuffle_intensity": "Standard",
        "skew_risk": False,
        "safety_buffer_pct": 5,
        "max_executors_cap": 400,
    },
}


def to_gb(value: float, unit: str) -> float:
    return value * 1024 if unit == "TB" else value


def round_up_to_512(memory_mb: int) -> int:
    return int(ceil(memory_mb / 512) * 512)


def choose_executor_cores(complexity: str, shuffle_intensity: str) -> int:
    """
    Golden rule: strictly 4 or 5 cores per executor.
    High or shuffle-heavy workloads prefer 4 cores for GC/network stability.
    """
    return 4 if complexity == "High" or shuffle_intensity != "Standard" else 5


def choose_aqe_partition_mb(complexity: str, shuffle_intensity: str) -> int:
    if complexity == "High" or shuffle_intensity == "Extreme":
        return 96
    return 128


def estimate_driver(complexity: str, skew_risk: bool, use_cache: bool) -> Tuple[int, int]:
    if complexity == "High":
        base_mb = 8192
        if skew_risk:
            base_mb += 2048
        return (base_mb, 2)
    if complexity == "Medium":
        return (6144 if use_cache else 4096, 1)
    return (3072, 1)


def calculate_resources(calc_input: CalcInput) -> CalcResult:
    format_factor = FORMAT_IN_MEMORY_FACTOR[calc_input.data_format]
    task_memory_mb = COMPLEXITY_TASK_MEMORY_MB[calc_input.complexity]
    throughput_mbps = COMPLEXITY_THROUGHPUT_MBPS[calc_input.complexity]
    shuffle_factor = SHUFFLE_MULTIPLIER[calc_input.shuffle_intensity]

    cache_multiplier = 1.30 if calc_input.use_cache else 1.0
    skew_multiplier = 1.15 if calc_input.skew_risk else 1.0
    safety_multiplier = 1.0 + (calc_input.safety_buffer_pct / 100)

    effective_data_mb = int(
        ceil(
            calc_input.data_volume_gb
            * GB_IN_MB
            * format_factor
            * shuffle_factor
            * safety_multiplier
        )
    )

    aqe_partition_mb = choose_aqe_partition_mb(calc_input.complexity, calc_input.shuffle_intensity)
    total_partitions = max(1, ceil(effective_data_mb / aqe_partition_mb))

    # SLA-based parallelism from required processing throughput.
    total_capacity_needed_mb_per_sec = effective_data_mb / (calc_input.sla_minutes * 60)
    required_cores_by_sla = max(1, ceil(total_capacity_needed_mb_per_sec / throughput_mbps))

    # Partition-pressure guardrail: practical amount of partitions per core for full DAG runtime.
    partitions_per_core_target = PARTITIONS_PER_CORE_TARGET[calc_input.complexity]
    required_cores_by_partition_pressure = max(1, ceil(total_partitions / partitions_per_core_target))

    required_parallel_tasks = max(required_cores_by_sla, required_cores_by_partition_pressure)
    executor_cores = choose_executor_cores(calc_input.complexity, calc_input.shuffle_intensity)
    raw_executors = max(1, ceil(required_parallel_tasks / executor_cores))
    executors = min(raw_executors, calc_input.max_executors_cap)

    # JVM executor memory model (Scala Spark):
    # task working set * concurrency + execution/cache pressure + JVM reserve.
    pressure_multiplier = cache_multiplier * shuffle_factor * skew_multiplier
    memory_for_tasks_mb = executor_cores * task_memory_mb * pressure_multiplier
    jvm_reserve_mb = 1536 if calc_input.complexity == "High" else 1024
    executor_memory_mb = round_up_to_512(max(4096, int(ceil(memory_for_tasks_mb + jvm_reserve_mb))))

    # Spark memory overhead for non-heap/off-heap/containers (JVM workloads).
    executor_overhead_mb = max(384, int(ceil(executor_memory_mb * 0.10)))

    driver_memory_mb, driver_cores = estimate_driver(
        complexity=calc_input.complexity,
        skew_risk=calc_input.skew_risk,
        use_cache=calc_input.use_cache,
    )

    total_cluster_cores = executors * executor_cores
    total_cluster_ram_mb = executors * (executor_memory_mb + executor_overhead_mb) + driver_memory_mb

    # Practical default for shuffle partitions with AQE enabled.
    shuffle_partitions = max(total_partitions, total_cluster_cores * 2)
    advisory_partition_bytes = aqe_partition_mb * MB_IN_BYTES

    rationale = (
        f"Cores/executor={executor_cores} выбран по правилу 4/5. "
        f"Параллелизм рассчитан от SLA и пропускной способности core, затем проверен "
        f"через pressure по партициям AQE ({total_partitions} партиций по ~{aqe_partition_mb}MB). "
        f"Память executor рассчитана только по JVM-модели Spark SQL (без PySpark), "
        f"с pressure-факторами cache/shuffle/skew и обязательным overhead=max(10%, 384MB)."
    )

    return CalcResult(
        executors=executors,
        executor_cores=executor_cores,
        executor_memory_mb=executor_memory_mb,
        executor_overhead_mb=executor_overhead_mb,
        driver_memory_mb=driver_memory_mb,
        driver_cores=driver_cores,
        total_cluster_cores=total_cluster_cores,
        total_cluster_ram_mb=total_cluster_ram_mb,
        effective_data_mb=effective_data_mb,
        aqe_partition_mb=aqe_partition_mb,
        total_partitions=total_partitions,
        required_parallel_tasks=required_parallel_tasks,
        shuffle_partitions=shuffle_partitions,
        advisory_partition_bytes=advisory_partition_bytes,
        rationale=rationale,
    )


def format_memory_gb(memory_mb: int) -> str:
    return f"{memory_mb / GB_IN_MB:.1f} GB"


def spark_submit_block(result: CalcResult) -> str:
    executor_memory_gb = max(1, ceil(result.executor_memory_mb / GB_IN_MB))
    driver_memory_gb = max(1, ceil(result.driver_memory_mb / GB_IN_MB))

    return (
        f"--num-executors {result.executors} \\\n"
        f"--executor-cores {result.executor_cores} \\\n"
        f"--executor-memory {executor_memory_gb}g \\\n"
        f"--driver-cores {result.driver_cores} \\\n"
        f"--driver-memory {driver_memory_gb}g \\\n"
        f"--conf spark.executor.memoryOverhead={result.executor_overhead_mb} \\\n"
        f"--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \\\n"
        f"--conf spark.sql.adaptive.enabled=true \\\n"
        f"--conf spark.sql.adaptive.coalescePartitions.enabled=true \\\n"
        f"--conf spark.sql.adaptive.advisoryPartitionSizeInBytes={result.advisory_partition_bytes} \\\n"
        f"--conf spark.sql.shuffle.partitions={result.shuffle_partitions} \\\n"
        f"--conf spark.default.parallelism={result.shuffle_partitions}"
    )


def profile_hint(profile_name: str) -> str:
    hints = {
        "Conservative": "Приоритет стабильности и headroom для пиковых нагрузок.",
        "Balanced": "Оптимальный режим по умолчанию для большинства прод ETL.",
        "Aggressive": "Максимум производительности при минимальном запасе ресурсов.",
    }
    return hints[profile_name]


def main() -> None:
    st.set_page_config(page_title="Spark 3.5.2 Resource Calculator", page_icon="⚙️", layout="wide")

    st.title("⚙️ Spark 3.5.2 Resource Calculator")
    st.caption("JVM-first sizing for Scala ETL pipelines (Spark 3.5.2, AQE-enabled, без PySpark overhead).")

    with st.sidebar:
        st.header("Business Inputs")
        profile_mode = st.radio("Sizing Profile", options=["Conservative", "Balanced", "Aggressive"], index=1)
        profile_values = PROFILE_DEFAULTS[profile_mode]
        st.caption(profile_hint(profile_mode))

        data_volume = st.number_input("Data Volume", min_value=1.0, value=100.0, step=10.0)
        volume_unit = st.selectbox("Unit", options=["GB", "TB"], index=0)
        data_format = st.selectbox("Data Format", options=["Parquet", "Iceberg", "ORC", "CSV"], index=0)
        complexity = st.selectbox("Transformation Complexity", options=["Low", "Medium", "High"], index=1)
        sla_minutes = st.slider("SLA (Minutes)", min_value=5, max_value=720, value=60, step=5)
        use_cache = st.checkbox("Caching/Persist intermediate DataFrames", value=False)

        st.subheader("Workload Profile")
        manual_override = st.checkbox("Override profile defaults", value=False)
        if manual_override:
            shuffle_intensity = st.selectbox("Shuffle Intensity", options=["Standard", "Heavy", "Extreme"], index=1)
            skew_risk = st.checkbox("Data Skew Risk", value=True)
        else:
            shuffle_intensity = str(profile_values["shuffle_intensity"])
            skew_risk = bool(profile_values["skew_risk"])
            st.write(f"- Shuffle Intensity: **{shuffle_intensity}**")
            st.write(f"- Data Skew Risk: **{'Yes' if skew_risk else 'No'}**")

        with st.expander("Advanced Guardrails", expanded=False):
            if manual_override:
                safety_buffer_pct = st.slider("Safety Buffer (%)", min_value=0, max_value=40, value=15, step=5)
                max_executors_cap = st.number_input("Max Executors Cap", min_value=1, value=250, step=1)
            else:
                safety_buffer_pct = int(profile_values["safety_buffer_pct"])
                max_executors_cap = int(profile_values["max_executors_cap"])
                st.write(f"- Safety Buffer: **{safety_buffer_pct}%**")
                st.write(f"- Max Executors Cap: **{max_executors_cap}**")

    data_volume_gb = to_gb(data_volume, volume_unit)
    calc_input = CalcInput(
        data_volume_gb=data_volume_gb,
        data_format=data_format,
        complexity=complexity,
        sla_minutes=sla_minutes,
        use_cache=use_cache,
        shuffle_intensity=shuffle_intensity,
        skew_risk=skew_risk,
        safety_buffer_pct=safety_buffer_pct,
        max_executors_cap=int(max_executors_cap),
    )

    result = calculate_resources(calc_input)

    row1 = st.columns(5)
    row1[0].metric("Total Executors", result.executors)
    row1[1].metric("Cores / Executor", result.executor_cores)
    row1[2].metric("RAM / Executor", format_memory_gb(result.executor_memory_mb))
    row1[3].metric("Driver RAM", format_memory_gb(result.driver_memory_mb))
    row1[4].metric("Driver Cores", result.driver_cores)

    row2 = st.columns(4)
    row2[0].metric("Total Cluster Cores", result.total_cluster_cores)
    row2[1].metric("Total Cluster RAM", format_memory_gb(result.total_cluster_ram_mb))
    row2[2].metric("Executor Overhead", f"{result.executor_overhead_mb} MB")
    row2[3].metric("Shuffle Partitions", result.shuffle_partitions)

    st.caption(f"Profile mode: {profile_mode}{' (manual override)' if manual_override else ''}")

    st.subheader("Ready-to-copy spark-submit")
    st.code(spark_submit_block(result), language="bash")

    with st.expander("Architect Notes", expanded=True):
        st.info(result.rationale)
        st.write(
            f"- Effective working data: **{result.effective_data_mb / GB_IN_MB:.1f} GB**.\n"
            f"- AQE advisory partition: **{result.aqe_partition_mb} MB**, total partitions: **{result.total_partitions}**.\n"
            f"- Required parallel task slots for SLA: **{result.required_parallel_tasks}**.\n"
            "- Модель использует только JVM Spark memory management для Scala ETL "
            "и не учитывает `spark.python.worker.memory`."
        )


if __name__ == "__main__":
    main()
