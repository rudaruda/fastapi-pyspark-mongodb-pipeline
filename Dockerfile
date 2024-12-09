FROM python:3.12-slim
# Log em tempo real
ENV PYTHONUNBUFFERED=1
# Definir o caminho do JAVA_HOME
ENV JAVA_HOME=/opt/java/openjdk
# Instala o JAVA
COPY --from=eclipse-temurin:11-jre $JAVA_HOME $JAVA_HOME
# Garante que o JAVA seja "considerado" pelo sistema
ENV PATH="${JAVA_HOME}/bin:${PATH}"
# Define WORKDIR
WORKDIR /code
# Copia requirements.txt para o root
COPY ./requirements.txt ./requirements.txt
# Instala bibliotecas do Python
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
# Copia pasta app
COPY ./app /code/app
# Cria pasta mongo_data para volune MongoDB
RUN mkdir -p data/mongo_data
# Garante que as bibliotecas Python foram instaladas
RUN pip install uvicorn fastapi pyspark pymongo
# Expõe porta 8000 para subir o serviço FastAPI/uvicorn
EXPOSE 8000
# Mostra variavel JAVA_HOME
RUN java --version
RUN echo $JAVA_HOME
# Start do serviço FastAPI
CMD ["uvicorn", "app.main:app", "--reload", "--host", "0.0.0.0", "--port", "8000"]