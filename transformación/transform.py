from datetime import datetime
import pandas as pd
import os
from langchain_community.chat_models import ChatOpenAI
from langchain.prompts import ChatPromptTemplate

def analyze_job_description(description, api_key):
    """Analyze job description using DeepSeek API"""
    chat = ChatOpenAI(
        openai_api_key=api_key,
        openai_api_base="https://api.deepseek.com/v1",
        model="deepseek-chat"
    )
    
    prompt_template = ChatPromptTemplate.from_template("""
    Analiza la siguiente descripción de trabajo y extrae la siguiente información:
    Nombre del puesto
    Empresa
    Lugar
    Tipo de contrato
    Experiencia deseada
    Soft skills
    Cloud Computing
    Big Data y Procesamiento
    Bases de Datos y Almacenamiento
    ETL/ELT y Automatización
    Programación y Scripts
    Visualización y BI
    DevOps y Control de Versiones
    Machine Learning e Inteligencia Artificial
    Link de publicación
    Fecha de publicación
    Nivel de puesto
    Industria
    Fuente de la publicación
    Salario estimado
    Fecha de cierre

    Si algún campo no está especificado, escribe "No especifica".

    Descripción:
    {description}
    """)
    
    prompt = prompt_template.format_messages(description=description)
    response = chat.invoke(prompt)
    return response.content

def transform_data(job_details, url, api_key):
    """Transform raw job details into structured DataFrame"""
    analysis = analyze_job_description(job_details, api_key)
    
    # Parse the analysis into a dictionary
    data = {}
    lines = analysis.split('\n')
    
    # Ignorar la primera línea si no contiene información útil
    start_index = 1
    
    for line in lines[start_index:]:
        if ':' in line:
            # Limpiar el nombre de la columna quitando prefijos no deseados
            key, value = line.split(':', 1)
            key = key.strip().lstrip('- **').rstrip('**')
            data[key] = value.strip()
    
    # Crear DataFrame con columnas requeridas
    df = pd.DataFrame([data])
    
    # Add URL and timestamp
    df['Link publicación'] = url
    df['Fecha analisis'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    return df

def save_to_parquet(dataframe, filename_prefix="jobs"):
    """Save DataFrame to parquet file in data folder"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"./data/parquet/{filename_prefix}_{timestamp}.parquet"
    
    os.makedirs("./data/parquet", exist_ok=True)
    dataframe.to_parquet(filename, engine='pyarrow')
    return filename

def save_to_json(dataframe, filename_prefix="jobs"):
    """Save DataFrame to JSON file in data folder"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"./data/job_details/{filename_prefix}_{timestamp}.json"
    
    os.makedirs("./data/job_details", exist_ok=True)
    dataframe.to_json(filename, orient="records", indent=2)
    return filename