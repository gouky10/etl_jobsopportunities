import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Cargar los datos de las tablas
actividades_df = pd.read_json('data/warehouse/actividades.json')
beneficios_df = pd.read_json('data/warehouse/beneficios.json')
principal_df = pd.read_json('data/warehouse/principal.json')
requerimientos_df = pd.read_json('data/warehouse/requerimientos.json')

# Configuración inicial
st.set_page_config(page_title='Dashboard de Oportunidades Laborales', layout='wide')
st.title('Dashboard de Oportunidades Laborales')

# Gráfico 1: Distribución de empresas por número de vacantes
empresas = principal_df['empresa'].value_counts()
fig1 = plt.figure(figsize=(10,6))
sns.countplot(x='empresa', data=principal_df, order=empresas.index)
plt.title('Distribución de vacantes por empresa')
plt.xticks(rotation=90)
st.pyplot(fig1)

# Gráfico 2: Tipos de contratos más comunes
contratos = principal_df['tipo_contrato'].value_counts().head(5)
fig2 = plt.figure(figsize=(8,6))
sns.countplot(x='tipo_contrato', data=principal_df, order=contratos.index)
plt.title('Tipos de contratos más comunes')
st.pyplot(fig2)

# Gráfico 3: Nivel de puesto más frecuente
niveles = principal_df['nivel_puesto'].value_counts().head(5)
fig3 = plt.figure(figsize=(8,6))
sns.countplot(x='nivel_puesto', data=principal_df, order=niveles.index)
plt.title('Niveles de puesto más frecuentes')
st.pyplot(fig3)

# Gráfico 4: Principales beneficios ofrecidos
beneficios = beneficios_df['beneficio'].value_counts().head(10)
fig4 = plt.figure(figsize=(10,6))
sns.countplot(x='beneficio', data=beneficios_df, order=beneficios.index)
plt.title('Principales beneficios ofrecidos')
plt.xticks(rotation=90)
st.pyplot(fig4)

# Gráfico 5: Requerimientos más comunes
requerimientos = requerimientos_df['tipo_requerimiento'].value_counts().head(5)
fig5 = plt.figure(figsize=(8,6))
sns.countplot(x='tipo_requerimiento', data=requerimientos_df, order=requerimientos.index)
plt.title('Requerimientos más comunes')
st.pyplot(fig5)

# Gráfico 6: Actividades más frecuentes en los puestos
actividades = actividades_df['actividad'].value_counts().head(10)
fig6 = plt.figure(figsize=(10,6))
sns.countplot(x='actividad', data=actividades_df, order=actividades.index)
plt.title('Actividades más frecuentes en los puestos')
plt.xticks(rotation=90)
st.pyplot(fig6)

# Tabla de requerimientos con filtrado y ordenación
st.subheader('Tabla de Requerimientos')
with st.spinner('Cargando tabla de requerimientos...'):
    requerimientos_df['tipo_requerimiento'] = requerimientos_df['tipo_requerimiento'].astype(str)
    required_columns = ['tipo_requerimiento', 'tecnologia', 'nivel_o_anos']
    df = requerimientos_df[required_columns].copy()
    df = df.dropna()
    
    # Filtros
    col1, col2, col3 = st.columns(3)
    with col1:
        tipo_filtro = st.multiselect('Filtrar por tipo de requerimiento', options=df['tipo_requerimiento'].unique(), default=None)
    with col2:
        tecnologia_filtro = st.multiselect('Filtrar por tecnología', options=df['tecnologia'].unique(), default=None)
    with col3:
        nivel_filtro = st.multiselect('Filtrar por nivel', options=df['nivel_o_anos'].unique(), default=None)
    
    # Aplicar filtros
    if tipo_filtro:
        df = df[df['tipo_requerimiento'].isin(tipo_filtro)]
    if tecnologia_filtro:
        df = df[df['tecnologia'].isin(tecnologia_filtro)]
    if nivel_filtro:
        df = df[df['nivel_o_anos'].isin(nivel_filtro)]
    
    # Ordenación
    col_order = st.selectbox('Ordenar por', options=['tipo_requerimiento', 'tecnologia', 'nivel_o_anos'])
    ascending = st.checkbox('Orden ascendente', value=True)
    
    df = df.sort_values(by=col_order, ascending=ascending)
    
    # Mostrar tabla
    st.subheader('Resultados filtrados y ordenados')
    st.dataframe(df, use_container_width=True, height=500)