import streamlit as st
import pandas as pd
import os
from glob import glob

def load_data():
    """Cargar todos los archivos JSON de la carpeta job_details"""
    # Obtener ruta absoluta del directorio data
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data", "job_details")
    files = glob(os.path.join(data_dir, "*.json"))
    
    if not files:
        print(f"No se encontraron archivos parquet en: {data_dir}")
    if not files:
        return pd.DataFrame()
    
    dfs = []
    for file in files:
        df = pd.read_json(file)
        dfs.append(df)
    
    return pd.concat(dfs, ignore_index=True)

def show_dashboard():
    st.set_page_config(page_title="Dashboard de Ofertas de Trabajo", layout="wide")
    
    st.title("Análisis de Ofertas de Trabajo")
    
    # Cargar datos
    data = load_data()
    
    if data.empty:
        st.warning("No hay datos disponibles")
        return
    
    # Mostrar datos
    st.subheader("Datos completos")
    st.dataframe(data)
    
    # Filtros
    st.sidebar.header("Filtros")
    
    # Filtro por empresa
    empresas = data['Empresa'].unique()
    selected_empresas = st.sidebar.multiselect(
        "Selecciona empresas",
        options=empresas,
        default=empresas
    )
    
    # Filtro por ubicación
    ubicaciones = data['Lugar'].unique()
    selected_ubicaciones = st.sidebar.multiselect(
        "Selecciona ubicaciones",
        options=ubicaciones,
        default=ubicaciones
    )
    
    # Aplicar filtros
    filtered_data = data[
        (data['Empresa'].isin(selected_empresas)) &
        (data['Lugar'].isin(selected_ubicaciones))
    ]
    
    # Mostrar datos filtrados
    st.subheader("Datos filtrados")
    st.dataframe(filtered_data)
    
    # Estadísticas
    st.subheader("Estadísticas")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total ofertas", len(filtered_data))
    
    with col2:
        st.metric("Empresas únicas", filtered_data['Empresa'].nunique())
    
    with col3:
        st.metric("Ubicaciones únicas", filtered_data['Lugar'].nunique())
    
    # Gráficos
    st.subheader("Distribución por Empresa")
    st.bar_chart(filtered_data['Empresa'].value_counts())
    
    st.subheader("Distribución por Ubicación")
    st.bar_chart(filtered_data['Lugar'].value_counts())

if __name__ == "__main__":
    show_dashboard()