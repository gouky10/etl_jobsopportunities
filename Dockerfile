# Usar una imagen base de Python
FROM python:3.9-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar el archivo de requisitos
COPY requirements.txt .

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Instalar Playwright y sus navegadores
RUN python -m playwright install

# Copiar el resto del código de la aplicación
COPY . .

# Comando por defecto
CMD ["python", "main.py"]