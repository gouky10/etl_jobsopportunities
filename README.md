# Proyecto ETL para Análisis de Ofertas Laborales

Este proyecto tiene como objetivo desarrollar un proceso ETL utilizando Python para analizar ofertas laborales publicadas en plataformas como LinkedIn, Indeed y FlexJobs. Este proceso extraerá, transformará y almacenará información clave de las ofertas laborales para visualizarlas en un dashboard interactivo, proporcionando insights sobre tendencias del mercado laboral y requisitos comunes.

## Estructura del Proyecto

- `scraping/`: Código para extraer datos de las plataformas.
- `transformacion/`: Código para transformar los datos extraídos.
- `almacenamiento/`: Código para almacenar los datos transformados.
- `visualizacion/`: Código para visualizar los datos en un dashboard.

## Configuración del Entorno

Para garantizar la portabilidad, se utiliza Docker. Para construir y ejecutar el contenedor, usa los siguientes comandos:

```sh
docker-compose build
docker-compose up