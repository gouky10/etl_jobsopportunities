from datetime import datetime
import pandas as pd
import os
import json
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
            api_key=API_KEY
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
    A partir de la oferta laboral que te comparto, extrae la información y organízala en las siguientes tablas en formato JSON:

    1. **Tabla Principal**:
       - Nombre del puesto
       - Empresa
       - Lugar
       - Tipo de contrato
       - Link de publicación
       - Fecha de publicación
       - Nivel de puesto
       - Industria
       - Fuente de la publicación
       - Salario estimado
       - Fecha de cierre

    2. **Tabla de Requerimientos**:
       - Tipo_Requerimiento (Cloud Computing, Big Data y Procesamiento, Bases de Datos y Almacenamiento, ETL/ELT y Automatización, Programación y Scripts, Visualización y BI, DevOps y Control de Versiones, Machine Learning e Inteligencia Artificial, Experiencia, Soft Skills, Idioma)
       - Tecnologia (herramientas, habilidades o idiomas específicos)
       - Nivel_o_Años (cantidad de años para experiencia o nivel de conocimiento para otros requerimientos; si no se especifica, colocar "No se especifica")

    3. **Tabla de Beneficios**:
       - Beneficio (cada beneficio debe estar en una fila separada)
       - Si no se mencionan beneficios, colocar "No se especifica".

    4. **Tabla de Actividades a Desarrollar**:
       - Actividad (cada actividad debe estar en una fila separada)
       - Si no se mencionan actividades, colocar "No se especifica".

    Pautas:
    - Cada requerimiento debe estar en una fila separada (no separar por comas).
    - Si no se menciona un requerimiento, no es necesario incluirlo en la tabla.
    - Para los campos `Nivel_o_Años`, si no se especifica, colocar "No se especifica".
    - Los beneficios y actividades deben extraerse textualmente de la oferta laboral y colocarse en filas separadas.
    - En tu respuesta SOLO responde con el formato JSON.

    Descripción:
    {description}
    """)
    
    prompt = prompt_template.format_messages(description=description)
    response = chat.invoke(prompt)
    return response

def transform_data(job_details, url):
    """Transform raw job details into structured JSON format"""
    analysis = analyze_job_description(job_details)
    
    # Debug: Show description and response JSON
    print("\nDescripción analizada:")
    print(job_details['description'])
    print("\nResponse JSON análisis:")
    print(analysis)

    # Limpiar y parsear el JSON
    try:
        # Limpiar formato Markdown
        clean_analysis = analysis.replace('```json\n', '').replace('```', '')
        # Parsear JSON
        response_json = json.loads(clean_analysis)

    except json.JSONDecodeError as e:
        print(f"Error al parsear JSON: {e}")
        print("\nResponse JSON que causó el error:")
        print(analysis)
        return None

    data = {
        "tabla_principal": [],
        "tabla_requerimientos": [],
        "tabla_beneficios": [],
        "tabla_actividades": []
    }

    # Parse principal table
    if "Tabla_Principal" in response_json:
        principal_data = {
            "nombre_del_puesto": response_json["Tabla_Principal"].get("Nombre_del_puesto", "No se especifica"),
            "empresa": response_json["Tabla_Principal"].get("Empresa", "No se especifica"),
            "lugar": response_json["Tabla_Principal"].get("Lugar", "No se especifica"),
            "tipo_contrato": response_json["Tabla_Principal"].get("Tipo_de_contrato", "No se especifica"),
            "link_publicacion": response_json["Tabla_Principal"].get("Link_de_publicación", "No se especifica"),
            "fecha_publicacion": response_json["Tabla_Principal"].get("Fecha_de_publicación", "No se especifica"),
            "nivel_puesto": response_json["Tabla_Principal"].get("Nivel_de_puesto", "No se especifica"),
            "industria": response_json["Tabla_Principal"].get("Industria", "No se especifica"),
            "fuente_publicacion": response_json["Tabla_Principal"].get("Fuente_de_la_publicación", "No se especifica"),
            "salario_estimado": response_json["Tabla_Principal"].get("Salario_estimado", "No se especifica"),
            "fecha_cierre": response_json["Tabla_Principal"].get("Fecha_de_cierre", "No se especifica")
        }
        data["tabla_principal"].append(principal_data)

    # Parse requirements table
    if "Tabla_de_Requerimientos" in response_json:
        for requirement in response_json["Tabla_de_Requerimientos"]:
            data["tabla_requerimientos"].append({
                "tipo_requerimiento": requirement.get("Tipo_Requerimiento", "No se especifica"),
                "tecnologia": requirement.get("Tecnologia", "No se especifica"),
                "nivel_o_años": requirement.get("Nivel_o_Años", "No se especifica")
            })

    # Parse benefits table
    if "Tabla_de_Beneficios" in response_json:
        for benefit in response_json["Tabla_de_Beneficios"]:
            data["tabla_beneficios"].append({
                "beneficio": benefit.get("Beneficio", "No se especifica")
            })

    # Parse activities table
    if "Tabla_de_Actividades_a_Desarrollar" in response_json:
        for activity in response_json["Tabla_de_Actividades_a_Desarrollar"]:
            data["tabla_actividades"].append({
                "actividad": activity.get("Actividad", "No se especifica")
            })

    return data

def save_to_parquet(dataframe, filename_prefix="jobs"):
    """Save DataFrame to parquet file in data folder"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"./data/parquet/{filename_prefix}_{timestamp}.parquet"
    
    os.makedirs("./data/parquet", exist_ok=True)
    dataframe.to_parquet(filename, engine='pyarrow')
    return filename

def save_to_json(data, filename_prefix="jobs"):
    """Save JSON data to JSON file in data/job_details folder"""
    if not data:
        print("No data to save")
        return None
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"./data/job_details/{filename_prefix}_{timestamp}.json"
    
    os.makedirs("./data/job_details", exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    return filename