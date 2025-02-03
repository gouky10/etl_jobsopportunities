from playwright.async_api import async_playwright
from datetime import datetime, timedelta
import re
from transformación.transform import transform_data, save_to_json

async def process_job(job_data):
    """Procesa un trabajo individual"""
    try:
        print("\n🚀 Iniciando el proceso de análisis del trabajo...")
        
        # Obtener detalles del trabajo
        print("✨ Obteniendo detalles del trabajo...")
        job_details = await scrape_job_details(job_data)
        if not job_details:
            print("❌ No se pudieron obtener los detalles del trabajo.")
            return None
        
        print("📝 Detalles obtenidos con éxito.")
        
        # Transformar y analizar datos
        print("🔍 Analizando la descripción del trabajo...")
        transformed_data = transform_data(job_details, job_data['link'])
        if transformed_data:
            print("📊 Análisis completado con éxito.")
        else:
            print("⚠️  El análisis no generó datos.")
        
        # Guardar resultados
        print("💾 Guardando los resultados...")
        output_file = save_to_json(transformed_data)
        print(f"✅ Datos guardados exitosamente en: {output_file}")
        
        return {
            'details': job_details,
            'transformed': transformed_data,
            'output_file': output_file
        }
        
    except Exception as e:
        print(f"❌ Error durante el proceso: {str(e)}")
        return None

async def scrape_job_details(job_data):
    """Scrape detailed job information from LinkedIn job posting"""
    try:
        print("\n🌐 Iniciando scraping de detalles del trabajo...")
        
        async with async_playwright() as p:
            browser = await p.firefox.launch(headless=False)
            page = await browser.new_page()
            await page.goto(job_data['link'])
            
            # Esperar a que el contenedor principal esté cargado
            await page.wait_for_selector('section.top-card-layout')
            print("📄 Página cargada correctamente.")
            
            # Extraer detalles principales
            print("🔍 Extrayendo información principal...")
            title = await page.query_selector('h1.top-card-layout__title')
            company = await page.query_selector('a.topcard__org-name-link')
            location = await page.query_selector('span.topcard__flavor--bullet')
            posted_date = await page.query_selector('span.posted-time-ago__text')
            applicants = await page.query_selector('span.num-applicants__caption')
            
            job_details = {
                **job_data,  # Incluir los datos básicos originales
                'title': await title.inner_text() if title else None,
                'company': await company.inner_text() if company else None,
                'location': await location.inner_text() if location else None,
                'posted_date': await posted_date.inner_text() if posted_date else None,
                'applicants': await applicants.inner_text() if applicants else None,
                'description': None
            }
            
            # Extraer ubicación, fecha y aplicantes
            print("📍 Actualizando ubicación, fecha y aplicantes...")
            primary_desc = await page.query_selector('div.job-details-jobs-unified-top-card__primary-description-container')
            if primary_desc:
                elements = await primary_desc.query_selector_all('span.tvm__text--low-emphasis')
                if len(elements) >= 3:
                    job_details['location'] = await elements[0].inner_text()
                    job_details['posted_date'] = await elements[1].inner_text()
                    job_details['applicants'] = await elements[2].inner_text()
            
            # Extraer descripción del trabajo
            print("📄 Extrayendo descripción del puesto...")
            await page.wait_for_selector('section.description')
            description = await page.query_selector('section.description')
            if description:
                job_details['description'] = await description.inner_text()
            
            print("✅ Detalles del trabajo extraídos con éxito.")
            await browser.close()
            return job_details
            
    except Exception as e:
        print(f"❌ Error scraping job details: {str(e)}")
        return None

def parse_posted_date(text):
    """Parse LinkedIn's relative date format"""
    if 'hora' in text or 'hour' in text:
        num = int(re.search(r'\d+', text).group())
        return datetime.now() - timedelta(hours=num)
    elif 'día' in text or 'day' in text:
        num = int(re.search(r'\d+', text).group())
        return datetime.now() - timedelta(days=num)
    elif 'semana' in text or 'week' in text:
        num = int(re.search(r'\d+', text).group())
        return datetime.now() - timedelta(weeks=num)
    else:
        return datetime.now()

if __name__ == "__main__":
    test_urls = [
        "https://www.linkedin.com/jobs/view/4114873404/",
        "https://www.linkedin.com/jobs/view/4085417600",
        "https://www.linkedin.com/jobs/view/4120604532/"
    ]
    
    for url in test_urls:
        details = scrape_job_details(url)
        if details:
            print(f"\nDetails for {url}:")
            for key, value in details.items():
                print(f"{key}: {value}")