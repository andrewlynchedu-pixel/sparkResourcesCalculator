import math

class SparkCapacityPlanner:
    """
    Профессиональный калькулятор ресурсов для Apache Spark (Capacity Planning).
    Основан на концепции вычислительных волн (execution waves) и пулах памяти Spark.
    """
    
    def __init__(self, block_size_mb=128, t_base_sec=25, e_mem_rate=3, cores_per_exec=5):
        # Базовые системные константы
        self.block_size_mb = block_size_mb   # Оптимальный размер партиции HDFS/Parquet
        self.t_base_sec = t_base_sec         # Базовое время простой задачи (сек)
        self.e_mem_rate = e_mem_rate         # Коэффициент расширения памяти (десериализация)
        self.cores_per_exec = cores_per_exec # Ядер на экзекьютор (Оптимально 4-5 для I/O HDFS)
        
        # Константы памяти Spark 
        self.reserved_memory_mb = 300        # Жестко зарезервировано Spark (MB)
        self.memory_fraction = 0.6           # spark.memory.fraction (Unified pool)
        self.overhead_fraction = 0.10        # spark.executor.memoryOverheadFactor
        self.min_overhead_mb = 384           # Минимальный overhead (MB)

    def calculate(self, data_size_gb, sla_seconds, k_comp=1.0, k_skew=1.0):
        """
        Расчет необходимых ресурсов для выполнения витрины в рамках SLA.
        
        :param data_size_gb: Объем входных данных (в ГБ)
        :param sla_seconds: Желаемое время выполнения (в секундах)
        :param k_comp: Коэффициент сложности расчетов (1.0 - просто, 2.0+ - тяжело)
        :param k_skew: Коэффициент перекоса данных (1.0 - нет перекоса, 1.5+ - есть перекос)
        """
        # 1. Расчет задач и времени
        total_data_mb = data_size_gb * 1024
        n_tasks = math.ceil(total_data_mb / self.block_size_mb)
        
        # Эффективное время выполнения одной задачи с учетом сложности и перекоса
        t_eff = self.t_base_sec * k_comp * k_skew
        
        # 2. Вычислительные волны (Waves)
        waves = math.floor(sla_seconds / t_eff)
        if waves < 1:
            raise ValueError(
                f"SLA ({sla_seconds} сек) слишком жесткий! Одна волна займет ~{t_eff:.1f} сек. "
                "Увеличьте SLA, оптимизируйте код или уменьшите партиции."
            )

        # 3. Расчет вычислительной мощности (vCPU)
        total_cores_needed = math.ceil(n_tasks / waves)
        n_executors = math.ceil(total_cores_needed / self.cores_per_exec)
        actual_total_cores = n_executors * self.cores_per_exec

        # 4. Расчет памяти (RAM)
        # Память для процессинга одной задачи (MB)
        m_task = self.block_size_mb * self.e_mem_rate * k_comp
        
        # Общая полезная память для всех потоков экзекьютора (Unified Memory)
        m_unified = m_task * self.cores_per_exec
        
        # Общий размер JVM Heap: Unified_Memory / 0.6 + 300 MB Reserved
        m_heap = math.ceil((m_unified / self.memory_fraction) + self.reserved_memory_mb)
        
        # Off-Heap (Overhead) память YARN/Kubernetes
        m_overhead = math.ceil(max(self.min_overhead_mb, m_heap * self.overhead_fraction))
        
        # Итоговая память контейнера (YARN Container / K8s Pod)
        m_container = m_heap + m_overhead
        m_container_gb = round(m_container / 1024, 2)
        total_cluster_mem_gb = round(m_container_gb * n_executors, 2)

        # 5. Подготовка отчета
        return self._generate_report(
            data_size_gb, sla_seconds, k_comp, k_skew, n_tasks, t_eff, waves, 
            n_executors, actual_total_cores, m_heap, m_overhead, m_container_gb, total_cluster_mem_gb
        )

    def _generate_report(self, data_gb, sla, k_comp, k_skew, tasks, t_eff, waves, n_exec, cores, heap, overhead, cont_gb, total_mem):
        report = f"""
=========================================================
🔥 SPARK CAPACITY PLANNING REPORT
=========================================================
ВХОДНЫЕ ПАРАМЕТРЫ:
- Данные: {data_gb} ГБ | SLA: {sla} сек ({sla/60:.1f} мин)
- Сложность (K_comp): {k_comp} | Перекос (K_skew): {k_skew}
---------------------------------------------------------
МЕТРИКИ ВЫПОЛНЕНИЯ:
- Всего задач (Tasks): {tasks:,}
- Эффективное время задачи: {t_eff:.1f} сек
- Требуемое кол-во волн: {waves}
---------------------------------------------------------
ТРЕБОВАНИЯ К КЛАСТЕРУ (YARN/K8s):
- Всего экзекьюторов: {n_exec}
- Всего ядер (vCPU): {cores} ({self.cores_per_exec} на экзекьютор)
- RAM на один контейнер: {cont_gb} ГБ
- Общая RAM кластера: {total_mem} ГБ
---------------------------------------------------------
SPARK CONFIGURATION (для spark-submit):
--num-executors {n_exec}
--executor-cores {self.cores_per_exec}
--executor-memory {heap}m
--conf spark.executor.memoryOverhead={overhead}m
=========================================================
"""
        return report

# === ПРИМЕР ИСПОЛЬЗОВАНИЯ ===
if __name__ == "__main__":
    # Инициализация планировщика с базовыми параметрами нашей системы
    planner = SparkCapacityPlanner(
        block_size_mb=128,  # Целевой размер партиции
        t_base_sec=25,      # Историческое базовое время простой таски
        e_mem_rate=3.0,     # Коэффициент разжатия Parquet в память JVM
        cores_per_exec=5    # Ядер на экзекьютор
    )

    try:
        # Просчет витрины: 500 ГБ, SLA 20 минут (1200 сек), сложные агрегации (1.2), небольшой перекос (1.3)
        result = planner.calculate(
            data_size_gb=500, 
            sla_seconds=1200, 
            k_comp=1.2, 
            k_skew=1.3
        )
        print(result)
    except ValueError as e:
        print(f"❌ ОШИБКА: {e}")