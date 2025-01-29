import pandas as pd
from scraping.scraper import scrape_job_details
from scraping.job_search import JobSearch
from transformación.transform import transform_data, save_to_parquet, save_to_json
from config.search_params import COUNTRIES
from datetime import datetime
import json
import os

def run_job_search(job_title: str, countries: list):
    """Ejecuta la búsqueda de trabajos y guarda los resultados como JSON"""
    
    # Inicializar el scraper
    scraper = JobSearch(headless=False)
    
    # Ejecutar el scraping
    print(f"Iniciando búsqueda para: {job_title}")
    results = scraper.scrape_jobs(job_title, countries)
    
    # Convertir a DataFrame
    df = pd.DataFrame(results)
    
    # Crear nombre de archivo con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"jobs_{timestamp}.json"
    filepath = os.path.join("data/job_searchs", filename)
    
    # Guardar como JSON
    df.to_json(filepath, orient="records", indent=2)
    print(f"Resultados guardados en: {filepath}")
    print(f"Total de trabajos encontrados: {len(df)}")

def scrape_single_job():
    """Extrae y procesa un solo trabajo"""
    try:
        # Configuración
        api_key = "sk-ebbddac5767c44af8a32b812dce49835"
        
        # Solicitar URL del trabajo
        url = input("Por favor ingresa la URL del trabajo: ")
        
        # Obtener detalles del trabajo
        print("Extrayendo detalles del trabajo...")
        job_details = scrape_job_details(url)
        
        # Transformar y analizar datos
        print("Analizando descripción del trabajo...")
        transformed_data = transform_data(job_details, url, api_key)
        
        # Guardar resultados
        output_file = save_to_json(transformed_data)
        print(f"Datos guardados exitosamente en {output_file}")
        
    except Exception as e:
        print(f"Error durante el proceso: {str(e)}")
        raise

def main():
    print("Selecciona el modo de operación:")
    print("1. Búsqueda masiva de trabajos")
    print("2. Análisis de un trabajo específico")
    
    choice = input("Ingresa tu elección (1 o 2): ")
    
    if choice == "1":
        job_title = input("Ingresa el título del trabajo a buscar: ")
        
        print("\nSelecciona el país para la búsqueda:")
        print("1. Perú")
        print("2. Estados Unidos")
        print("3. Ambos")
        
        country_choice = input("Ingresa tu elección (1, 2 o 3): ")
        
        if country_choice == "1":
            countries = ["PE"]
        elif country_choice == "2":
            countries = ["US"]
        elif country_choice == "3":
            countries = ["PE", "US"]
        else:
            print("Opción no válida, se usará Perú por defecto")
            countries = ["PE"]
            
        run_job_search(job_title, countries)
    elif choice == "2":
        scrape_single_job()
    else:
        print("Opción no válida")

if __name__ == "__main__":
    main()