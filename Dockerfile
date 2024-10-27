FROM python:3.12-slim

WORKDIR /app

COPY . .

EXPOSE 50051

RUN pip install -r requirements.txt

RUN python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. cloudberry_storage.proto

RUN ls -R /app

ENV PYTHONPATH="${PYTHONPATH}:/app"

CMD ["python", "main.py"]