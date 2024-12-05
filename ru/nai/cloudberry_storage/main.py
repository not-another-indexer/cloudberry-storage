import os
import sys

import grpc
from concurrent import futures
import time

# добавляем папку 'generated' в путь
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../generated'))

from ru.nai.cloudberry_storage.generated import cloudberry_storage_pb2_grpc as pb2_grpc
from ru.nai.cloudberry_storage.controller import CloudberryStorageService


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_CloudberryStorageServicer_to_server(CloudberryStorageService(), server)

    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server is running on port 50051...")

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()
