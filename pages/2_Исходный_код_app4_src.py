from pathlib import Path

import streamlit as st


st.set_page_config(page_title="Исходный код app4_src", page_icon="📄", layout="wide")
st.title("📄 Исходный код: app4_src.py")
st.caption("Просмотр исходного кода расчетной модели в режиме read-only")

root_dir = Path(__file__).resolve().parents[1]
source_file = root_dir / "app4_src.py"

if not source_file.exists():
    st.error("Файл app4_src.py не найден в корне проекта.")
else:
    code_text = source_file.read_text(encoding="utf-8")
    st.code(code_text, language="python")
