from datetime import datetime
import pandas as pd
import os
from openai import OpenAI
from langchain_core.language_models import BaseLLM
from langchain_core.outputs import LLMResult, Generation
from config.api_keys import API_KEY

class OpenRouteLLM(BaseLLM):
    base_url: str = "https://openrouter.ai/api/v1"
    model: str = "deepseek/deepseek-r1-distill-llama-70b"

    @property
    def _llm_type(self) -> str:
        return "openroute"

    def _call(self, prompt: str, **kwargs) -> str:
        return self._generate([prompt], **kwargs).generations[0][0].text

    def _generate(self, prompts: list[str], **kwargs) -> LLMResult:
        client = OpenAI(
            base_url=self.base_url,
            api_key = API_KEY
        )
        
        results = []
        for prompt in prompts:
            completion = client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            )
            
            text = completion.choices[0].message.content
            results.append([Generation(text=text)])
        
        return LLMResult(generations=results)

from langchain.prompts import ChatPromptTemplate

def analyze_job_description(description):
    """Analyze job description using DeepSeek API"""
    chat = OpenRouteLLM()
    
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
    return response

def transform_data(job_details, url):
    """Transform raw job details into structured DataFrame"""
    analysis = analyze_job_description(job_details)
    
    # Parse the analysis into a dictionary
    data = {}
    lines = analysis.split('\n')
    
    # Ignorar la primera línea si no contiene información útil
    start_index = 1
    
    for line in lines[start_index:]:
        if ':' in line:
            # Limpiar el nombre de la columna quitando prefijos no deseados
            key, value = line.split(':', 1)
            key = key.strip().lstrip('- **').rstrip('*')
            value = value.lstrip('*').strip()
            data[key] = value
    
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