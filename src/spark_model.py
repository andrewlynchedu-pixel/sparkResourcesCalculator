from __future__ import annotations

from dataclasses import dataclass
from math import ceil


@dataclass
class SparkSizingInput:
    app_name: str
    input_data_gb: float
    compression_expand_factor: float
    format_overhead_factor: float
    transform_amplification_factor: float
    shuffle_ratio: float
    skew_factor: float
    join_strategy: str
    stages_count: int
    input_partitions: int
    shuffle_partitions: int
    udf_complexity_factor: float

    executors: int
    cores_per_executor: int
    executor_memory_gb: float
    executor_memory_overhead_pct: float
    cluster_cpu_utilization: float

    cpu_throughput_mb_s_per_core: float
    disk_throughput_mb_s_per_executor: float
    network_throughput_mb_s_per_executor: float

    spark_memory_fraction: float
    spark_storage_fraction: float
    active_working_set_fraction: float
    cache_fraction_of_data: float
    broadcast_tables_gb: float
    concurrent_shuffle_fraction: float

    stage_scheduler_overhead_s: float
    spill_penalty_per_100pct: float
    gc_penalty_sensitivity: float
    expected_retry_fraction: float
    retry_cost_fraction: float
    speculative_execution_gain: float

    target_runtime_min: float
    min_executors_for_search: int
    max_executors_for_search: int


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _join_multiplier(strategy: str) -> float:
    multipliers = {
        "Broadcast hash join": 0.35,
        "Sort-merge join": 1.0,
        "Shuffle hash join": 0.85,
        "Mostly aggregations (few joins)": 0.6,
    }
    return multipliers.get(strategy, 1.0)


def estimate_spark_resources(config: SparkSizingInput) -> dict:
    input_logical_gb = (
        config.input_data_gb
        * config.compression_expand_factor
        * config.format_overhead_factor
    )

    post_transform_gb = input_logical_gb * config.transform_amplification_factor
    shuffled_gb = (
        post_transform_gb
        * config.shuffle_ratio
        * config.skew_factor
        * _join_multiplier(config.join_strategy)
    )
    total_processed_gb = post_transform_gb + shuffled_gb

    total_cores = config.executors * config.cores_per_executor
    effective_cores = total_cores * _clamp(config.cluster_cpu_utilization, 0.2, 1.0)

    cpu_throughput = config.cpu_throughput_mb_s_per_core / _clamp(
        config.udf_complexity_factor, 0.2, 10.0
    )
    total_processed_mb = total_processed_gb * 1024.0
    cpu_seconds = total_processed_mb / max(1.0, cpu_throughput * max(effective_cores, 1.0))

    disk_seconds = (input_logical_gb * 1024.0) / max(
        1.0, config.disk_throughput_mb_s_per_executor * max(config.executors, 1)
    )
    network_seconds = (shuffled_gb * 1024.0) / max(
        1.0, config.network_throughput_mb_s_per_executor * max(config.executors, 1)
    )

    tasks_total = max(config.input_partitions, config.shuffle_partitions, total_cores)
    waves = tasks_total / max(total_cores, 1)
    wave_penalty = 1.0 + max(0.0, waves - 1.0) * 0.025

    base_pipeline_seconds = max(cpu_seconds, disk_seconds, network_seconds)
    scheduling_overhead_seconds = config.stages_count * config.stage_scheduler_overhead_s

    total_executor_heap_gb = config.executors * config.executor_memory_gb
    execution_memory_gb = (
        total_executor_heap_gb
        * config.spark_memory_fraction
        * (1.0 - config.spark_storage_fraction)
    )
    storage_memory_gb = (
        total_executor_heap_gb
        * config.spark_memory_fraction
        * config.spark_storage_fraction
    )

    working_set_gb = post_transform_gb * config.active_working_set_fraction
    cache_need_gb = post_transform_gb * config.cache_fraction_of_data
    shuffle_in_flight_gb = shuffled_gb * config.concurrent_shuffle_fraction
    peak_execution_need_gb = (
        working_set_gb + shuffle_in_flight_gb + config.broadcast_tables_gb
    )

    spill_ratio = max(0.0, peak_execution_need_gb - execution_memory_gb) / max(
        execution_memory_gb, 0.001
    )
    spill_multiplier = 1.0 + spill_ratio * config.spill_penalty_per_100pct

    heap_pressure = (peak_execution_need_gb + cache_need_gb) / max(total_executor_heap_gb, 0.001)
    gc_multiplier = 1.0 + max(0.0, heap_pressure - 0.65) * config.gc_penalty_sensitivity

    retry_multiplier = 1.0 + config.expected_retry_fraction * config.retry_cost_fraction
    speculative_multiplier = 1.0 - _clamp(config.speculative_execution_gain, 0.0, 0.35)

    estimated_runtime_seconds = (
        (base_pipeline_seconds * wave_penalty + scheduling_overhead_seconds)
        * spill_multiplier
        * gc_multiplier
        * retry_multiplier
        * speculative_multiplier
    )

    estimated_runtime_min = estimated_runtime_seconds / 60.0

    executor_memory_overhead_gb = (
        config.executor_memory_gb * config.executor_memory_overhead_pct / 100.0
    )
    total_memory_gb_with_overhead = config.executors * (
        config.executor_memory_gb + executor_memory_overhead_gb
    )

    cpu_core_hours = (estimated_runtime_seconds / 3600.0) * total_cores

    zero_spill_execution_memory_needed_gb = max(peak_execution_need_gb, 0.001)
    required_heap_total_gb_for_zero_spill = zero_spill_execution_memory_needed_gb / max(
        config.spark_memory_fraction * (1.0 - config.spark_storage_fraction), 0.001
    )
    recommended_executor_memory_gb = required_heap_total_gb_for_zero_spill / max(
        config.executors, 1
    )

    storage_pressure_ratio = cache_need_gb / max(storage_memory_gb, 0.001)

    return {
        "input_logical_gb": input_logical_gb,
        "post_transform_gb": post_transform_gb,
        "shuffled_gb": shuffled_gb,
        "total_processed_gb": total_processed_gb,
        "total_cores": total_cores,
        "effective_cores": effective_cores,
        "cpu_seconds": cpu_seconds,
        "disk_seconds": disk_seconds,
        "network_seconds": network_seconds,
        "waves": waves,
        "wave_penalty": wave_penalty,
        "scheduling_overhead_seconds": scheduling_overhead_seconds,
        "spill_ratio": spill_ratio,
        "spill_multiplier": spill_multiplier,
        "heap_pressure": heap_pressure,
        "gc_multiplier": gc_multiplier,
        "retry_multiplier": retry_multiplier,
        "estimated_runtime_min": estimated_runtime_min,
        "cpu_core_hours": cpu_core_hours,
        "execution_memory_gb": execution_memory_gb,
        "storage_memory_gb": storage_memory_gb,
        "working_set_gb": working_set_gb,
        "cache_need_gb": cache_need_gb,
        "shuffle_in_flight_gb": shuffle_in_flight_gb,
        "peak_execution_need_gb": peak_execution_need_gb,
        "total_memory_gb_with_overhead": total_memory_gb_with_overhead,
        "recommended_executor_memory_gb": recommended_executor_memory_gb,
        "storage_pressure_ratio": storage_pressure_ratio,
    }


def runtime_for_executors(config: SparkSizingInput, executors: int) -> float:
    adjusted = SparkSizingInput(**{**config.__dict__, "executors": int(executors)})
    return estimate_spark_resources(adjusted)["estimated_runtime_min"]


def recommend_executors_for_sla(config: SparkSizingInput) -> int | None:
    if config.target_runtime_min <= 0:
        return None

    lower = max(1, config.min_executors_for_search)
    upper = max(lower, config.max_executors_for_search)

    best = None
    for executors in range(lower, upper + 1):
        runtime_min = runtime_for_executors(config, executors)
        if runtime_min <= config.target_runtime_min:
            best = executors
            break

    return best


def suggest_partitions(total_cores: int) -> int:
    return max(64, int(ceil(total_cores * 2.5)))
