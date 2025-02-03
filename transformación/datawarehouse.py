import pandas as pd
import os
import json
from typing import Dict, Any

class DataWarehouse:
    def __init__(self):
        self.job_id = 1
        self.data_path = "data/job_details"
        self.wh_path = "data/warehouse"
        self.tables = {
            "principal": [],
            "requerimientos": [],
            "beneficios": [],
            "actividades": []
        }
        
        # Crear directorio de warehouse si no existe
        if not os.path.exists(self.wh_path):
            os.makedirs(self.wh_path)

    def process_job_details(self):
        """Procesar todos los archivos JSON en data/job_details"""
        for filename in os.listdir(self.data_path):
            if filename.endswith(".json"):
                job_path = os.path.join(self.data_path, filename)
                with open(job_path, 'r', encoding='utf-8') as f:
                    job_data = json.load(f)
                    self.process_job(job_data)
                    self.job_id += 1

    def process_job(self, job_data: Dict[str, Any]):
        """Procesar un solo trabajo y cargar los datos en las tablas"""
        # Tabla Principal
        principal_data = job_data.get('tabla_principal', [{}])[0]
        principal = {
            "job_id": self.job_id,
            "nombre_puesto": principal_data.get("nombre_puesto", "No se especifica"),
            "empresa": principal_data.get("empresa", "No se especifica"),
            "lugar": principal_data.get("lugar", "No se especifica"),
            "tipo_contrato": principal_data.get("tipo_contrato", "No se especifica"),
            "link_publicacion": principal_data.get("link_publicacion", "No se especifica"),
            "fecha_publicacion": principal_data.get("fecha_publicacion", "No se especifica"),
            "nivel_puesto": principal_data.get("nivel_puesto", "No se especifica"),
            "industria": principal_data.get("industria", "No se especifica"),
            "fuente_publicacion": principal_data.get("fuente_publicacion", "No se especifica"),
            "salario_estimado": principal_data.get("salario_estimado", "No se especifica"),
            "fecha_cierre": principal_data.get("fecha_cierre", "No se especifica")
        }
        self.tables["principal"].append(principal)

        # Tabla de Requerimientos
        for requerimiento in job_data.get("tabla_requerimientos", []):
            requirement = {
                "tipo_requerimiento": requerimiento.get("tipo_requerimiento", "No se especifica"),
                "tecnologia": requerimiento.get("tecnologia", "No se especifica"),
                "nivel_o_anos": requerimiento.get("nivel_o_anos", "No se especifica")
            }
            self.tables["requerimientos"].append({
                "job_id": self.job_id,
                **requirement
            })

        # Tabla de Beneficios
        for beneficio in job_data.get("tabla_beneficios", []):
            self.tables["beneficios"].append({
                "job_id": self.job_id,
                "beneficio": beneficio.get("beneficio", "No se especifica")
            })

        # Tabla de Actividades a Desarrollar
        for actividad in job_data.get("tabla_actividades", []):
            self.tables["actividades"].append({
                "job_id": self.job_id,
                "actividad": actividad.get("actividad", "No se especifica")
            })

    def process_requirement(self, requirement: Dict[str, Any]):
        """Procesar un requerimiento y cargar en la tabla de requerimientos"""
        requerimiento = {
            "job_id": self.job_id,
            "tipo_requerimiento": requirement.get("type", "No se especifica"),
            "tecnologia": requirement.get("technology", "No se especifica"),
            "nivel_o_anos": requirement.get("level_years", "No se especifica")
        }
        self.tables["requerimientos"].append(requerimiento)

    def process_benefit(self, benefit: Dict[str, Any]):
        """Procesar un beneficio y cargar en la tabla de beneficios"""
        beneficio = {
            "job_id": self.job_id,
            "beneficio": benefit.get("name", "No se especifica")
        }
        self.tables["beneficios"].append(beneficio)

    def process_activity(self, activity: Dict[str, Any]):
        """Procesar una actividad y cargar en la tabla de actividades"""
        actividad = {
            "job_id": self.job_id,
            "actividad": activity.get("name", "No se especifica")
        }
        self.tables["actividades"].append(actividad)

    def save_tables(self):
        """Guardar las tablas en formato JSON en data/warehouse"""
        os.makedirs(self.wh_path, exist_ok=True)
        
        for table_name, table_data in self.tables.items():
            if table_data:
                table_path = os.path.join(self.wh_path, f"{table_name}.json")
                with open(table_path, 'w', encoding='utf-8') as f:
                    json.dump(table_data, f, indent=4, ensure_ascii=False)
        
        self.tables = {
            "principal": [],
            "requerimientos": [],
            "beneficios": [],
            "actividades": []
        }

def main():
    warehouse = DataWarehouse()
    warehouse.process_job_details()
    warehouse.save_tables()

if __name__ == "__main__":
    main()