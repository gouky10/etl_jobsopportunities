from playwright.sync_api import sync_playwright
from datetime import datetime, timedelta
import re

def scrape_job_details(url):
    """Scrape detailed job information from LinkedIn job posting"""
    try:
        with sync_playwright() as p:
            browser = p.firefox.launch(headless=False)
            page = browser.new_page()
            page.goto(url)
            
            # Esperar a que el contenedor principal esté cargado
            page.wait_for_selector('section.top-card-layout')
            
            # Extraer detalles principales
            job_details = {
                'title': page.query_selector('h1.top-card-layout__title').inner_text().strip(),
                'company': page.query_selector('a.topcard__org-name-link').inner_text().strip(),
                'location': page.query_selector('span.topcard__flavor--bullet').inner_text().strip(),
                'posted_date': page.query_selector('span.posted-time-ago__text').inner_text().strip(),
                'applicants': page.query_selector('span.num-applicants__caption').inner_text().strip(),
                'url': url
            }
            
            # Extraer ubicación, fecha y aplicantes
            primary_desc = page.query_selector('div.job-details-jobs-unified-top-card__primary-description-container')
            if primary_desc:
                elements = primary_desc.query_selector_all('span.tvm__text--low-emphasis')
                if len(elements) >= 3:
                    job_details['location'] = elements[0].inner_text().strip()
                    job_details['posted_date'] = elements[1].inner_text().strip()
                    job_details['applicants'] = elements[2].inner_text().strip()
            
            # Extraer descripción del trabajo
            page.wait_for_selector('section.description')
            description = page.query_selector('section.description')
            if description:
                job_details['description'] = description.inner_text().strip()
            
            browser.close()
            return job_details
            
    except Exception as e:
        print(f"Error scraping job details: {str(e)}")
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