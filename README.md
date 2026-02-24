# Spark Resources Calculator (Streamlit)

Интерактивный калькулятор ресурсов Spark-приложения с оценкой:

- времени выполнения;
- требуемой RAM;
- CPU (cores, core-hours);
- риска spill и GC pressure;
- рекомендаций по числу executors под целевое SLA.

## Запуск

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Что учитывается в модели

- объём входных данных, сжатие и формат;
- доля shuffle, skew, тип join и сложность трансформаций;
- количество stages и накладные расходы scheduler;
- пропускная способность CPU/IO/network;
- параметры executors (cores, memory, overhead);
- память под execution/storage, broadcast, кэширование;
- spill, GC penalty, retries, speculative execution.

## Важно

Это инженерная оценочная модель, а не замена профилирования реального workload в Spark UI/History Server.
Для production рекомендуется калибровать коэффициенты по фактическим метрикам кластера.
# sparkResourcesCalculator
