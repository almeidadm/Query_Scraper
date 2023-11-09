FROM apache/airflow:slim-2.7.0-python3.10

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ibglib2.0-0 \
    libnss3 \            
    libnspr4 \           
    libatk1.0-0 \        
    libatk-bridge2.0-0 \ 
    libcups2 \           
    libdrm2 \            
    libdbus-1-3\        
    libxcb1\            
    libxkbcommon0\      
    libatspi2.0-0\      
    libx11-6\           
    libxcomposite1\     
    libxdamage1\        
    libxext6\           
    libxfixes3\         
    libxrandr2\         
    libgbm1\            
    libpango-1.0-0\     
    libcairo2\          
    libasound2 \       
    locales && \
    sed -i -e 's/# pt_BR.UTF-8 UTF-8/pt_BR.UTF-8 UTF-8/' /etc/locale.gen && \
    dpkg-reconfigure --frontend=noninteractive locales && \
    apt-get update && \
    apt-get clean

ENV LANG pt_BR.UTF-8
ENV LC_ALL pt_BR.UTF-8


USER airflow

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir apache-airflow[celery,redis,postgres]==2.7.0 \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.0/constraints-3.8.txt"
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install