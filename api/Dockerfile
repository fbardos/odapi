# Use the official Python image as a base image
FROM python:3.10-slim

# Set environment variables to avoid Python writing .pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install dependencies needed for PostgreSQL
RUN apt-get update && \
    apt-get install -y libpq-dev gcc && \
    apt-get install -y --no-install-recommends \
        git \
        build-essential \
        wget \
        gdal-bin \
        libgdal-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install miniconda
ENV CONDA_DIR=/opt/conda
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-py310_24.7.1-0-Linux-x86_64.sh -O ~/miniconda.sh \
    && /bin/bash ~/miniconda.sh -b -p $CONDA_DIR \
    && rm ~/miniconda.sh
ENV PATH=$CONDA_DIR/bin:$PATH

# Must be placed after conda install, otherwise, will load wrong version
COPY ./environment.yml /tmp/environment.yml
RUN conda env create --solver libmamba -f /tmp/environment.yml && conda clean -ay

# Change shell
SHELL ["conda", "run", "-n", "odapi", "/bin/bash", "-c"]

# Set the working directory in the container
WORKDIR /app

# Copy the rest of the application code into the container
COPY api/ .

# Expose the port FastAPI will run on
EXPOSE 80

# Run the FastAPI application using uvicorn
CMD ["conda", "run", "--no-capture-output", "-n", "odapi", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80", "--workers", "4"]


