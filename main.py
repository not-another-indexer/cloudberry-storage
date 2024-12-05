import grpc
from concurrent import futures
import time

from controller import CloudberryStorageService
import cloudberry_storage_pb2_grpc

import sys
from os.path import dirname
sys.path.append(dirname("ru/nai/cloudberry_storage/generated"))

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    cloudberry_storage_pb2_grpc.add_CloudberryStorageServicer_to_server(CloudberryStorageService(), server)

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
