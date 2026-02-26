import math
from pathlib import Path

import pandas as pd
import streamlit as st


class SparkCapacityPlanner:
    """
    Калькулятор ресурсов для Apache Spark (Capacity Planning).
    Основан на концепции execution waves и пулах памяти Spark.
    """

    def __init__(
        self,
        block_size_mb: int = 128,
        t_base_sec: float = 25,
        e_mem_rate: float = 3.0,
        cores_per_exec: int = 5,
        skew_memory_mode: str = "off",
    ):
        self.block_size_mb = block_size_mb
        self.t_base_sec = t_base_sec
        self.e_mem_rate = e_mem_rate
        self.cores_per_exec = cores_per_exec
        self.skew_memory_mode = skew_memory_mode

        self.reserved_memory_mb = 300
        self.memory_fraction = 0.6
        self.overhead_fraction = 0.10
        self.min_overhead_mb = 384

    def _memory_skew_factor(self, k_skew: float) -> float:
        if self.skew_memory_mode == "linear":
            return k_skew
        if self.skew_memory_mode == "sqrt":
            return math.sqrt(k_skew)
        return 1.0

    def calculate(
        self,
        data_size_gb: float,
        sla_seconds: int,
        k_comp: float = 1.0,
        k_skew: float = 1.0,
    ) -> dict:
        self._validate_inputs(data_size_gb, sla_seconds, k_comp, k_skew)

        total_data_mb = data_size_gb * 1024
        n_tasks = math.ceil(total_data_mb / self.block_size_mb)

        t_eff = self.t_base_sec * k_comp * k_skew

        waves = math.floor(sla_seconds / t_eff)
        if waves < 1:
            raise ValueError(
                f"SLA ({sla_seconds} сек) слишком жесткий. "
                f"Одна волна занимает ~{t_eff:.1f} сек."
            )

        total_cores_needed = math.ceil(n_tasks / waves)
        n_executors = math.ceil(total_cores_needed / self.cores_per_exec)
        actual_total_cores = n_executors * self.cores_per_exec

        memory_skew_factor = self._memory_skew_factor(k_skew)
        m_task = self.block_size_mb * self.e_mem_rate * k_comp * memory_skew_factor
        m_unified = m_task * self.cores_per_exec
        m_heap = math.ceil((m_unified / self.memory_fraction) + self.reserved_memory_mb)
        m_overhead = math.ceil(max(self.min_overhead_mb, m_heap * self.overhead_fraction))

        m_container = m_heap + m_overhead
        m_container_gb = round(m_container / 1024, 2)
        total_cluster_mem_gb = round((m_container * n_executors) / 1024, 2)

        return {
            "inputs": {
                "data_size_gb": data_size_gb,
                "sla_seconds": sla_seconds,
                "k_comp": k_comp,
                "k_skew": k_skew,
            },
            "execution": {
                "tasks": n_tasks,
                "t_eff": round(t_eff, 2),
                "waves": waves,
            },
            "cluster": {
                "executors": n_executors,
                "total_cores": actual_total_cores,
                "cores_per_executor": self.cores_per_exec,
                "container_memory_gb": m_container_gb,
                "total_cluster_memory_gb": total_cluster_mem_gb,
            },
            "memory": {
                "m_task_mb": round(m_task, 2),
                "m_unified_mb": round(m_unified, 2),
                "executor_heap_mb": m_heap,
                "executor_overhead_mb": m_overhead,
                "container_memory_mb": m_container,
                "skew_memory_mode": self.skew_memory_mode,
                "memory_skew_factor": round(memory_skew_factor, 3),
            },
            "spark_submit": {
                "num_executors": n_executors,
                "executor_cores": self.cores_per_exec,
                "executor_memory": f"{m_heap}m",
                "memory_overhead": f"{m_overhead}m",
            },
        }

    def _validate_inputs(
        self,
        data_size_gb: float,
        sla_seconds: int,
        k_comp: float,
        k_skew: float,
    ) -> None:
        if data_size_gb <= 0:
            raise ValueError("Объем данных должен быть > 0")
        if sla_seconds <= 0:
            raise ValueError("SLA должен быть > 0")
        if k_comp <= 0:
            raise ValueError("K_comp должен быть > 0")
        if k_skew <= 0:
            raise ValueError("K_skew должен быть > 0")
        if self.block_size_mb <= 0 or self.t_base_sec <= 0 or self.e_mem_rate <= 0 or self.cores_per_exec <= 0:
            raise ValueError("Системные параметры должны быть > 0")


def format_spark_submit(cfg: dict) -> str:
    return (
        "spark-submit \\\n"
        f"  --num-executors {cfg['num_executors']} \\\n"
        f"  --executor-cores {cfg['executor_cores']} \\\n"
        f"  --executor-memory {cfg['executor_memory']} \\\n"
        f"  --conf spark.executor.memoryOverhead={cfg['memory_overhead']}"
    )


def render_calculator_page() -> None:
    st.title("🔥 Spark Capacity Planner")
    st.caption("Профессиональный калькулятор расчета ресурсов Spark под SLA")

    with st.sidebar:
        st.subheader("Входные параметры")

        data_size_gb = st.number_input("Данные (ГБ)", min_value=1.0, value=500.0, step=10.0)
        sla_min = st.number_input("SLA (мин)", min_value=1, value=20, step=1)

        k_comp = st.slider("K_comp (сложность)", min_value=1.0, max_value=3.0, value=1.2, step=0.1)
        k_skew = st.slider("K_skew (перекос)", min_value=1.0, max_value=3.0, value=1.3, step=0.1)

        st.markdown("---")
        st.subheader("Системные настройки")
        block_size_mb = st.selectbox("Block size (MB)", options=[64, 96, 128, 192, 256], index=2)
        t_base_sec = st.number_input("Base task time (сек)", min_value=1.0, value=25.0, step=1.0)
        e_mem_rate = st.slider("E_mem_rate", min_value=1.0, max_value=6.0, value=3.0, step=0.1)
        cores_per_exec = st.selectbox("Cores per executor", options=[2, 3, 4, 5, 6, 8], index=3)

        skew_memory_mode = st.selectbox(
            "Учет skew в памяти",
            options=["off", "sqrt", "linear"],
            index=0,
            help="off: без влияния skew на RAM; sqrt: умеренно; linear: консервативно",
        )

    planner = SparkCapacityPlanner(
        block_size_mb=block_size_mb,
        t_base_sec=t_base_sec,
        e_mem_rate=e_mem_rate,
        cores_per_exec=cores_per_exec,
        skew_memory_mode=skew_memory_mode,
    )

    try:
        result = planner.calculate(
            data_size_gb=data_size_gb,
            sla_seconds=int(sla_min * 60),
            k_comp=k_comp,
            k_skew=k_skew,
        )

        exec_metrics = result["execution"]
        cluster = result["cluster"]
        memory = result["memory"]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Executors", f"{cluster['executors']}")
        c2.metric("Total vCPU", f"{cluster['total_cores']}")
        c3.metric("RAM / контейнер", f"{cluster['container_memory_gb']} ГБ")
        c4.metric("RAM / кластер", f"{cluster['total_cluster_memory_gb']} ГБ")

        st.markdown("### Метрики выполнения")
        m1, m2, m3 = st.columns(3)
        m1.metric("Tasks", f"{exec_metrics['tasks']:,}")
        m2.metric("t_eff", f"{exec_metrics['t_eff']} сек")
        m3.metric("Waves (доступно в SLA)", f"{exec_metrics['waves']}")

        st.markdown("### Детали памяти")
        memory_df = pd.DataFrame(
            [
                {"Параметр": "m_task (MB)", "Значение": memory["m_task_mb"]},
                {"Параметр": "m_unified (MB)", "Значение": memory["m_unified_mb"]},
                {"Параметр": "executor_heap (MB)", "Значение": memory["executor_heap_mb"]},
                {"Параметр": "memoryOverhead (MB)", "Значение": memory["executor_overhead_mb"]},
                {"Параметр": "container_memory (MB)", "Значение": memory["container_memory_mb"]},
                {"Параметр": "skew_memory_mode", "Значение": memory["skew_memory_mode"]},
                {"Параметр": "memory_skew_factor", "Значение": memory["memory_skew_factor"]},
            ]
        )
        memory_df["Значение"] = memory_df["Значение"].astype(str)
        st.dataframe(memory_df, width="stretch", hide_index=True)

        st.markdown("### Spark submit")
        submit_cmd = format_spark_submit(result["spark_submit"])
        st.code(submit_cmd, language="bash")

        with st.expander("Пояснение модели"):
            st.markdown(
                """
- Время: `t_eff = t_base_sec × K_comp × K_skew`
- Волны: `waves = floor(SLA / t_eff)`
- Ядра: `ceil(tasks / waves)`
- Память: расчет через Unified Memory (`spark.memory.fraction = 0.6`) и `memoryOverhead`
- Итоговая RAM кластера считается из MB и округляется только в конце
                """
            )

    except ValueError as exc:
        st.error(f"Ошибка расчета: {exc}")


def render_source_page() -> None:
    st.title("📄 Исходный код: app4_src.py")
    st.caption("Просмотр исходного кода расчетной модели в режиме read-only")

    source_file = Path(__file__).resolve().parent / "app4_src.py"
    if not source_file.exists():
        st.error("Файл app4_src.py не найден в корне проекта.")
        return

    st.code(source_file.read_text(encoding="utf-8"), language="python")


def render_description_page() -> None:
    st.title("📘 Описание калькулятора")
    st.caption("Теория и практическая интерпретация всех параметров модели")

    description_file = Path(__file__).resolve().parent / "docs" / "calculator_description.md"
    if not description_file.exists():
        st.error("Файл описания не найден: docs/calculator_description.md")
        return

    st.markdown(description_file.read_text(encoding="utf-8"))


def render_icon_nav() -> str:
    if "page" not in st.session_state:
        st.session_state.page = "calc"

    def set_page(page_name: str) -> None:
        st.session_state.page = page_name

    c1, c2, c3 = st.columns(3)
    with c1:
        st.button(
            "🧮",
            key="nav_calc",
            help="Калькулятор",
            width="stretch",
            type="primary" if st.session_state.page == "calc" else "secondary",
            on_click=set_page,
            args=("calc",),
        )
    with c2:
        st.button(
            "📄",
            key="nav_source",
            help="Код",
            width="stretch",
            type="primary" if st.session_state.page == "source" else "secondary",
            on_click=set_page,
            args=("source",),
        )
    with c3:
        st.button(
            "📘",
            key="nav_about",
            help="Описание",
            width="stretch",
            type="primary" if st.session_state.page == "about" else "secondary",
            on_click=set_page,
            args=("about",),
        )

    return st.session_state.page


def build_ui() -> None:
    st.set_page_config(
        page_title="Spark Capacity Calculator",
        page_icon="🔥",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.markdown(
        """
        <style>
        section[data-testid="stSidebar"] div.block-container {
            padding-top: 0 !important;
        }
        section[data-testid="stSidebar"] div.stButton > button {
            min-height: 34px;
            height: 34px;
            padding: 0;
            border-radius: 10px;
            font-size: 20px;
            line-height: 1;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        nav = render_icon_nav()

    if nav == "calc":
        render_calculator_page()
    elif nav == "source":
        render_source_page()
    else:
        render_description_page()


if __name__ == "__main__":
    build_ui()
