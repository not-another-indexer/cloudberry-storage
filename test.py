import grpc
from ru.nai.cloudberry_storage.generated import cloudberry_storage_pb2_grpc as pb2_grpc
from generated import cloudberry_storage_pb2 as pb2

def test_init_bucket(channel):
    stub = pb2_grpc.CloudberryStorageStub(channel)
    response = stub.InitBucket(pb2.Bucket(bucket_uuid="test_bucket"))
    print("InitBucket Response:", response)

def test_put_entry(channel):
    stub = pb2_grpc.CloudberryStorageStub(channel)
    metadata = pb2.PutEntryRequest.Metadata(
        bucket_uuid="test_bucket",
        content_uuid="test_content",
        extension="jpg",
        description="Test image"
    )
    request_iterator = iter([
        pb2.PutEntryRequest(metadata=metadata),
        pb2.PutEntryRequest(chunk_data=b"fake_image_data")  # Заглушка для данных изображения
    ])
    response = stub.PutEntry(request_iterator)
    print("PutEntry Response:", response)

def test_find(channel):
    stub = pb2_grpc.CloudberryStorageStub(channel)
    query = pb2.FindRequest(
        bucket_uuid="test_bucket",
        query="test query",
        count=5
    )
    response = stub.Find(query)
    print("Find Response:")
    for entry in response.entries:
        print(entry)

if __name__ == "__main__":
    with grpc.insecure_channel("localhost:50051") as channel:
        print("Testing InitBucket...")
        test_init_bucket(channel)

        print("\nTesting PutEntry...")
        test_put_entry(channel)

        print("\nTesting Find...")
        test_find(channel)
