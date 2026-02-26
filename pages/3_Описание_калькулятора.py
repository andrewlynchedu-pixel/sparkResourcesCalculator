from pathlib import Path

import streamlit as st


st.set_page_config(page_title="Описание калькулятора", page_icon="📘", layout="wide")
st.title("📘 Описание калькулятора")
st.caption("Теория и практическая интерпретация всех параметров модели")

root_dir = Path(__file__).resolve().parents[1]
description_file = root_dir / "docs" / "calculator_description.md"

if not description_file.exists():
    st.error("Файл описания не найден: docs/calculator_description.md")
else:
    description = description_file.read_text(encoding="utf-8")
    st.markdown(description)
