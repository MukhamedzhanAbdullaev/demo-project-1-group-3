FROM python:3.9

WORKDIR /app

COPY /app .

COPY requirements.txt .

RUN pip install -r requirements.txt

ENV API_KEY=652b9374613cc3a722ed4f2e1e03d7e18ef96073
ENV SERVER_NAME=project1-aq-postgres-rds.clqggm8eskug.us-east-1.rds.amazonaws.com
ENV DATABASE_NAME=air_quality
ENV DB_USERNAME=postgres
ENV DB_PASSWORD=RRHtOvWqAm1uXG9d0Km56-N!urM8
ENV PORT=5432
ENV LOGGING_SERVER_NAME=project1-aq-postgres-rds.clqggm8eskug.us-east-1.rds.amazonaws.com
ENV LOGGING_DATABASE_NAME=data_warehouse
ENV LOGGING_USERNAME=postgres
ENV LOGGING_PASSWORD=RRHtOvWqAm1uXG9d0Km56-N!urM8
ENV LOGGING_PORT=5432

CMD ["python", "-m", "project.pipelines.air_quality"]