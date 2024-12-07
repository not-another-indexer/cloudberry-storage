import grpc
from ru.nai.cloudberry_storage import cloudberry_storage_pb2_grpc as pb2_grpc
from ru.nai.cloudberry_storage import cloudberry_storage_pb2 as pb2
from ru.nai.cloudberry_storage.cloudberry_storage_pb2 import Coefficient, Parameter

TEST_BUCKET_UUID = "550e8400-e29b-41d4-a716-446655440005"
TEST_CONTENT_UUID = "550e8400-e29b-41d4-a716-446655440020"
TEST1_CONTENT_UUID = "550e8400-e29b-41d4-a716-446655440016"
TEST1_1CONTENT_UUID = "550e8400-e29b-41d4-a716-446655440017"
TEST2_CONTENT_UUID = "550e8400-e29b-41d4-a716-446655440018"


def test_init_bucket(channel):
    stub = pb2_grpc.CloudberryStorageStub(channel)
    request = pb2.InitBucketRequest(p_bucket_uuid=TEST_BUCKET_UUID)
    response = stub.InitBucket(request)
    print("InitBucket Response:", response)


def test_put_entry(channel, bucket_id, content_id):
    stub = pb2_grpc.CloudberryStorageStub(channel)
    metadata = pb2.ContentMetadata(
        p_bucket_uuid=bucket_id,
        p_content_uuid=TEST_CONTENT_UUID,
        p_extension="png",
        p_description="Test image"
    )
    request = pb2.PutEntryRequest(
        p_metadata=metadata,
        p_data=b"fake_image_data"  # Заглушка для данных изображения
    )
    response = stub.PutEntry(request)
    print("PutEntry Response:", response)


def test_put_entry_with_real_image(channel, bucket_id, content_id, image_path):
    stub = pb2_grpc.CloudberryStorageStub(channel)

    # Читаем реальное изображение из файла
    with open(image_path, "rb") as image_file:
        image_data = image_file.read()

    metadata = pb2.ContentMetadata(
        p_bucket_uuid=bucket_id,
        p_content_uuid=content_id,
        p_extension="png",
        p_description="Harry Potter, Rone, Hermione"
    )
    request = pb2.PutEntryRequest(
        p_metadata=metadata,
        p_data=image_data
    )
    response = stub.PutEntry(request)
    print("PutEntry Response:", response)


def test_find(channel):
    stub = pb2_grpc.CloudberryStorageStub(channel)
    query = pb2.FindRequest(
        p_bucket_uuid=TEST_BUCKET_UUID,
        p_query="Graphic plot Aknowledge",
        p_parameters=[Coefficient(p_parameter=Parameter.SEMANTIC_ONE_PEACE_SIMILARITY, p_value=0.9),
                      Coefficient(p_parameter=Parameter.RECOGNIZED_TEXT_SIMILARITY, p_value=0.1)],
        p_count=5
    )
    response = stub.Find(query)
    print("Find Response:")
    for entry in response.p_entries:
        print(entry)


if __name__ == "__main__":
    with grpc.insecure_channel("176.123.160.174:8002") as channel:
        # print("Testing InitBucket...")
        # test_init_bucket(channel)

        # test_put_entry_with_real_image(channel, TEST_BUCKET_UUID, TEST_CONTENT_UUID,"C:\\Users\\sckwo\\PycharmProjects\\cloudberry-storage\\ru\\nai\\cloudberry_storage\\test.png")

        print("\nTesting Find...")
        test_find(channel)
