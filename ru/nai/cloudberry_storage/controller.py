import grpc
from concurrent import futures
import sys

sys.path.append('../../../generated')
from generated import cloudberry_storage_pb2_grpc as pb2_grpc
from generated import cloudberry_storage_pb2 as pb2


class CloudberryStorageServicer(pb2_grpc.CloudberryStorageServicer):
    buckets = {}

    def InitBucket(self, request, context):
        bucket_uuid = request.bucket_uuid
        if bucket_uuid in self.buckets:
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            context.set_details(f"Bucket {bucket_uuid} already exists.")
            return pb2.Empty()
        else:
            self.buckets[bucket_uuid] = {}
            print(f"Initialized bucket: {bucket_uuid}")
            return pb2.Empty()

    def DestroyBucket(self, request, context):
        bucket_uuid = request.bucket_uuid
        if bucket_uuid in self.buckets:
            del self.buckets[bucket_uuid]
            print(f"Destroyed bucket: {bucket_uuid}")
            return pb2.Empty()
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Bucket {bucket_uuid} not found.")
            return pb2.Empty()

    def PutEntry(self, request_iterator, context):
        current_metadata = None
        for req in request_iterator:
            if req.HasField('metadata'):
                current_metadata = req.metadata
                bucket_uuid = current_metadata.bucket_uuid
                if bucket_uuid not in self.buckets:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details(f"Bucket {bucket_uuid} not found.")
                    return pb2.Empty()
                self.buckets[bucket_uuid][current_metadata.content_uuid] = {
                    'metadata': current_metadata,
                    'data': b''
                }
                print(f"Metadata received for content {current_metadata.content_uuid}")
            if req.HasField('chunk_data'):
                content_uuid = current_metadata.content_uuid
                self.buckets[bucket_uuid][content_uuid]['data'] += req.chunk_data
                print(f"Chunk data received for content {content_uuid}, size: {len(req.chunk_data)}")
        return pb2.Empty()

    def RemoveEntry(self, request, context):
        bucket_uuid = request.bucket_uuid
        content_uuid = request.content_uuid
        if bucket_uuid in self.buckets and content_uuid in self.buckets[bucket_uuid]:
            del self.buckets[bucket_uuid][content_uuid]
            print(f"Removed entry {content_uuid} from bucket {bucket_uuid}")
            return pb2.Empty()
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Entry {content_uuid} in bucket {bucket_uuid} not found.")
            return pb2.Empty()

    def Find(self, request, context):
        bucket_uuid = request.bucket_uuid
        query = request.query
        if bucket_uuid not in self.buckets:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Bucket {bucket_uuid} not found.")
            return pb2.FindResponse()

        print(f"Searching in bucket {bucket_uuid} with query: {query}")

        response = pb2.FindResponse()
        for content_uuid, entry in self.buckets[bucket_uuid].items():
            if query.lower() in entry['metadata'].description.lower():
                response_entry = pb2.FindResponseEntry(
                    content_uuid=content_uuid,
                    metrics=[
                        pb2.Metric(parameter=pb2.Parameter.SEMANTIC_ONE_PEACE_SIMILARITY, value=0.95)
                    ]
                )
                response.entries.append(response_entry)

        if not response.entries:
            print(f"No results found for query: {query}")
        else:
            print(f"Found {len(response.entries)} result(s) for query: {query}")

        return response


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_CloudberryStorageServicer_to_server(CloudberryStorageServicer(), server)
    server.add_insecure_port('[::]:50051')
    print("Server started on port 50051")
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()