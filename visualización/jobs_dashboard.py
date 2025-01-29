import streamlit as st
import pandas as pd
import os
from glob import glob
import json
from datetime import datetime

def load_jobs_data():
    """Carga los archivos JSON de trabajos de la carpeta job_searchs"""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data", "job_searchs")
    
    # Crear carpeta si no existe
    os.makedirs(data_dir, exist_ok=True)
    
    files = glob(os.path.join(data_dir, "*.json"))
    
    if not files:
        print(f"No se encontraron archivos JSON en: {data_dir}")
        return pd.DataFrame()
    
    dfs = []
    for file in files:
        try:
            df = pd.read_json(file)
            dfs.append(df)
        except Exception as e:
            print(f"Error leyendo archivo {file}: {str(e)}")
            continue
    
    if not dfs:
        return pd.DataFrame()
    
    # Unir todos los DataFrames
    df = pd.concat(dfs, ignore_index=True)
    
    # Convertir columnas de fecha
    date_columns = ['posted', 'fecha_analisis']  # Agregar otras columnas de fecha si es necesario
    for col in date_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception as e:
                print(f"Error convirtiendo columna {col} a datetime: {str(e)}")
    
    return df

def main():
    st.set_page_config(page_title="Dashboard de Trabajos", layout="wide")
    
    st.title("📊 Dashboard de Trabajos de LinkedIn")
    
    # Cargar datos
    df = load_jobs_data()
    
    # Filtros
    st.sidebar.header("Filtros")
    
    # Filtro por ubicación
    locations = df["location"].unique()
    selected_locations = st.sidebar.multiselect(
        "Selecciona ubicaciones", 
        options=locations,
        default=locations
    )
    
    # Filtro por empresa
    companies = df["company"].unique()
    selected_companies = st.sidebar.multiselect(
        "Selecciona empresas",
        options=companies,
        default=companies
    )
    
    # Filtro por fecha
    if "posted" in df.columns:
        min_date = df["posted"].min()
        max_date = df["posted"].max()
        
        date_range = st.sidebar.date_input(
            "Rango de fechas",
            value=[min_date, max_date],
            min_value=min_date,
            max_value=max_date
        )
    
    # Aplicar filtros
    filtered_df = df[
        (df["location"].isin(selected_locations)) &
        (df["company"].isin(selected_companies))
    ]
    
    if "posted" in df.columns:
        filtered_df = filtered_df[
            (filtered_df["posted"] >= pd.to_datetime(date_range[0])) &
            (filtered_df["posted"] <= pd.to_datetime(date_range[1]))
        ]
    
    # Mostrar datos
    st.header("Trabajos Filtrados")
    st.dataframe(filtered_df)
    
    # Métricas
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Trabajos", len(filtered_df))
    col2.metric("Empresas Únicas", filtered_df["company"].nunique())
    col3.metric("Ubicaciones Únicas", filtered_df["location"].nunique())
    
    # Gráficos
    st.header("Distribución de Trabajos")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Por Empresa")
        company_counts = filtered_df["company"].value_counts()
        st.bar_chart(company_counts)
    
    with col2:
        if "posted" in filtered_df.columns:
            st.subheader("Por Fecha")
            date_counts = filtered_df["posted"].dt.date.value_counts().sort_index()
            st.line_chart(date_counts)
    
    # Exportar datos
    st.sidebar.header("Exportar Datos")
    if st.sidebar.button("Exportar a CSV"):
        csv = filtered_df.to_csv(index=False).encode("utf-8")
        st.sidebar.download_button(
            label="Descargar CSV",
            data=csv,
            file_name="trabajos_filtrados.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main()