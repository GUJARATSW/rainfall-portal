# app.py

import streamlit as st
import pandas as pd
import subprocess
import os
from io import BytesIO

st.set_page_config(page_title="Rainfall & Water Level Portal", layout="wide")
st.title("🌧️ Rainfall & Water Level Station Portal")

# 🔘 Button to fetch fresh data
if st.button("🔄 Fetch Latest Data"):
    with st.spinner("Fetching data from India-Water.gov.in..."):
        result = subprocess.run(["python", "WIMS_Data_Excel.py"], capture_output=True, text=True)
        if result.returncode == 0:
            st.success("✅ Data fetched successfully.")
        else:
            st.error("❌ Failed to fetch data.")
            st.text(result.stderr)

# 📂 Check if Excel exists
excel_file = "WIMS_Gujarat_SWDC_Test.xlsx"
if os.path.exists(excel_file):
    df = pd.read_excel(excel_file)

    # 📊 Sidebar filters
    st.sidebar.header("📊 Filter Data")
    districts = st.sidebar.multiselect("District", options=sorted(df["District"].dropna().unique()))
    station_types = st.sidebar.multiselect("Station Type", options=sorted(df["Station Type"].dropna().unique()))

    filtered = df.copy()
    if districts:
        filtered = filtered[filtered["District"].isin(districts)]
    if station_types:
        filtered = filtered[filtered["Station Type"].isin(station_types)]

    # 🧾 Show filtered data
    st.write("### Filtered Station Data")
    st.dataframe(filtered, use_container_width=True)

    # 📥 Excel download button
    def to_excel_bytes(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        return output

    excel_bytes = to_excel_bytes(filtered)
    st.download_button(
        label="⬇️ Download Excel",
        data=excel_bytes,
        file_name="filtered_data.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.warning("⚠️ No data file found. Please fetch data first.")
