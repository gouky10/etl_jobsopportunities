import pandas as pd
from scraping.scraper import scrape_job_details, process_job
from scraping.job_search import JobSearch
from transformación.transform import transform_data, save_to_parquet, save_to_json
from config.search_params import COUNTRIES
from datetime import datetime
import json
import os
import asyncio

async def run_job_search(job_title: str, countries: list):
    """Ejecuta la búsqueda de trabajos y guarda los resultados como JSON"""
    
    # Inicializar el scraper
    scraper = JobSearch()
    await scraper.__ainit__(headless=False)
    
    # Ejecutar el scraping
    print(f"Iniciando búsqueda para: {job_title}")
    results = await scraper.scrape_jobs(job_title, countries)
    
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

async def scrape_single_job():
    """Extrae y procesa un solo trabajo"""
    try:
        job_data = {}
        job_data['link'] = input("Por favor ingresa la URL del trabajo: ")
        
        # Procesar el trabajo usando la función helper
        result = await process_job(job_data)
            
    except Exception as e:
        print(f"Error durante el proceso: {str(e)}")
        raise

async def main():
    print("Selecciona el modo de operación:")
    print("1. Búsqueda masiva de trabajos")
    print("2. Análisis de un trabajo específico")
    print("3. Procesar búsqueda histórica")
    
    choice = input("Ingresa tu elección (1, 2 o 3): ")
    
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
        
        await run_job_search(job_title, countries)
    elif choice == "2":
        await scrape_single_job()
    elif choice == "3":
        # Listar archivos de búsquedas históricas
        search_dir = "data/job_searchs"
        files = [f for f in os.listdir(search_dir) if f.endswith('.json')]
        
        if not files:
            print("No hay búsquedas históricas disponibles")
            return
            
        print("\nBúsquedas históricas disponibles:")
        for i, file in enumerate(files, 1):
            print(f"{i}. {file}")
            
        file_choice = int(input("Seleccione una búsqueda: ")) - 1
        selected_file = os.path.join(search_dir, files[file_choice])
        
        # Función para mostrar los registros con paginación
        def mostrar_registros(registros, cantidad=10):
            """Muestra los primeros N registros con paginación"""
            total = len(registros)
            pagina = 1
            while True:
                inicio = (pagina - 1) * cantidad
                fin = inicio + cantidad
                print(f"\nRegistros del {inicio + 1} al {min(fin, total)}:")
                for i in range(inicio, fin):
                    if i >= total:
                        break
                    registro = registros[i]
                    print(f"{i + 1}. {registro['title']} - {registro['company']}")
    
                ver_mas = input("\n¿Desea ver más registros? (s/n): ")
                if ver_mas.lower() != 's':
                    break
                pagina += 1
                if inicio + cantidad >= total:
                    print("\nNo hay más registros para mostrar.")
                    break
        
        # Función para obtener el índice de inicio
        def obtener_inicio(registros):
            """Obtiene el índice de inicio para el procesamiento"""
            while True:
                try:
                    inicio = int(input("\n¿A partir de qué registro desea comenzar el procesamiento? "))
                    if 1 <= inicio <= len(registros):
                        return inicio - 1  # Convertimos a índice cero
                    else:
                        print("Por favor ingrese un número válido entre 1 y", len(registros))
                except ValueError:
                    print("Por favor ingrese un número válido.")
        
        # Cargar y mostrar los registros
        with open(selected_file, 'r', encoding='utf-8') as archivo:
            registros = json.load(archivo)
        
        # Mostrar los registros con paginación
        mostrar_registros(registros)
        
        # Obtener el índice de inicio
        inicio_proceso = obtener_inicio(registros)
        
        # Definir el número total de trabajos a procesar
        num_jobs = int(input("Cuantos registros se va a procesar: ")) 
        
        # Procesar la búsqueda seleccionada
        job_search = await JobSearch.create()
        results = await job_search.process_historical_search(selected_file, num_jobs, start_index=inicio_proceso)
        
        if results:
            print("\nResultados procesados:")
    else:
        print("Opción no válida")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())