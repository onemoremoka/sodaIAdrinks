FROM python:3.10-slim
# set the working directory
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# update pip and install setuptools and wheel
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# copy the requirements file to the container and install dependencies

# API
COPY requirements.txt .
RUN pwd
RUN pip install --no-cache-dir -r requirements.txt

# copy the actual application code to the container
COPY . .

EXPOSE 8000

CMD ["uvicorn", "apilab.main:app", "--host", "0.0.0.0", "--port", "8000"]