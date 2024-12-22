# Use the official Python image as a base image
FROM python:3.10-slim

# Set environment variables to avoid Python writing .pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install dependencies needed for PostgreSQL
RUN apt-get update && \
    apt-get install -y libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt into the container and install dependencies
#COPY requirements.txt .
RUN pip install --no-cache-dir \
    pandas \
    "fastapi[standard]" \
    fastapi-pagination \
    uvicorn \
    "sqlalchemy[asyncio]==1.4" \
    asyncpg \
    python-dotenv \
    "geopandas>=1.0" \
    openpyxl \
    geoalchemy2 \
    psycopg2

# Copy the rest of the application code into the container
COPY . .

# Expose the port FastAPI will run on
EXPOSE 80

# Run the FastAPI application using uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80", "--workers", "4"]

