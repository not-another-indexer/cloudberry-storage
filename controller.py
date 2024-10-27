import grpc
from concurrent import futures
import re
import uuid
import json
import os
import logging
import base64

import cloudberry_storage_pb2_grpc, cloudberry_storage_pb2

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("cloudberry_storage.log"), logging.StreamHandler()])


def is_valid_uuid(value):
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False


def is_valid_image_extension(extension):
    valid_extensions = {'jpg', 'jpeg', 'png'}
    return extension.lower() in valid_extensions


def is_plain_text(text):
    return bool(re.match("^[A-Za-z0-9_ .,!?'-]*$", text))


class CloudberryStorageService(cloudberry_storage_pb2_grpc.CloudberryStorageServicer):
    BUCKETS_FILE = "buckets.json"

    def __init__(self):
        self.buckets = self.load_buckets()

    def load_buckets(self):
        if os.path.exists(self.BUCKETS_FILE):
            with open(self.BUCKETS_FILE, 'r') as f:
                return json.load(f)
        return {}

    def save_buckets(self):
        with open(self.BUCKETS_FILE, 'w') as file:
            json.dump(self.buckets, file)

    def InitBucket(self, request, context):
        bucket_uuid = request.p_bucket_uuid

        try:
            if not is_valid_uuid(bucket_uuid):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"Invalid UUID: {bucket_uuid}.")
                return cloudberry_storage_pb2.Empty()

            if bucket_uuid in self.buckets:
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                context.set_details(f"Bucket {bucket_uuid} already exists.")
                return cloudberry_storage_pb2.Empty()
            else:
                self.buckets[bucket_uuid] = {}
                logging.info(f"Initialized bucket: {bucket_uuid}")
                self.save_buckets()  # Save changes to the file
                return cloudberry_storage_pb2.Empty()
        except Exception as e:
            logging.error(f"Error initializing bucket {bucket_uuid}: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("An internal error occurred.")
            return cloudberry_storage_pb2.Empty()

    def DestroyBucket(self, request, context):
        bucket_uuid = request.p_bucket_uuid

        try:
            if not is_valid_uuid(bucket_uuid):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"Invalid UUID: {bucket_uuid}.")
                return cloudberry_storage_pb2.Empty()

            if bucket_uuid in self.buckets:
                del self.buckets[bucket_uuid]
                logging.info(f"Destroyed bucket: {bucket_uuid}")
                self.save_buckets()  # Save changes to the file
                return cloudberry_storage_pb2.Empty()
            else:
                context.set_code(grpc.StatusCode.OK)
                context.set_details(f"Bucket {bucket_uuid} not found.")
                return cloudberry_storage_pb2.Empty()
        except Exception as e:
            logging.error(f"Error destroying bucket {bucket_uuid}: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("An internal error occurred.")
            return cloudberry_storage_pb2.Empty()

    def PutEntry(self, request, context):
        content_uuid = request.p_metadata.p_content_uuid
        bucket_uuid = request.p_metadata.p_bucket_uuid
        file_extension = request.p_metadata.p_extension
        file_content = request.p_data
        description = request.p_metadata.p_description

        try:
            if not is_valid_image_extension(file_extension):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"Invalid file extension: {file_extension}.")
                return cloudberry_storage_pb2.Empty()

            if not file_content:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("File content cannot be empty.")
                return cloudberry_storage_pb2.Empty()

            if not is_plain_text(description):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Description must be plain text.")
                return cloudberry_storage_pb2.Empty()

            if bucket_uuid not in self.buckets:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Bucket {bucket_uuid} not found.")
                return cloudberry_storage_pb2.Empty()

            bucket = self.buckets[bucket_uuid]
            if content_uuid in bucket:
                context.set_code(grpc.StatusCode.OK)
                context.set_details(f"Content {content_uuid} already exists.")
                return cloudberry_storage_pb2.Empty()

            self.buckets[bucket_uuid][content_uuid] = {
                "extension": file_extension,
                "content": base64.b64encode(file_content).decode('utf-8'),
                "description": description
            }
            logging.info(f"Added entry {content_uuid} to bucket {bucket_uuid}")
            self.save_buckets()

            return cloudberry_storage_pb2.Empty()
        except Exception as e:
            logging.error(f"Error adding entry {content_uuid} to bucket {bucket_uuid}: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("An internal error occurred.")
            return cloudberry_storage_pb2.Empty()

    def RemoveEntry(self, request, context):
        bucket_uuid = request.p_bucket_uuid
        content_uuid = request.p_content_uuid

        try:
            if not is_valid_uuid(bucket_uuid) or not is_valid_uuid(content_uuid):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"Invalid UUIDs: bucket_uuid={bucket_uuid}, content_uuid={content_uuid}.")
                return cloudberry_storage_pb2.Empty()

            if bucket_uuid in self.buckets and content_uuid in self.buckets[bucket_uuid]:
                del self.buckets[bucket_uuid][content_uuid]
                logging.info(f"Removed entry {content_uuid} from bucket {bucket_uuid}")
                self.save_buckets()  # Save changes to the file
                return cloudberry_storage_pb2.Empty()
            else:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Entry {content_uuid} in bucket {bucket_uuid} not found.")
                return cloudberry_storage_pb2.Empty()
        except Exception as e:
            logging.error(f"Error removing entry {content_uuid} from bucket {bucket_uuid}: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("An internal error occurred.")
            return cloudberry_storage_pb2.Empty()

    def Find(self, request, context):
        bucket_uuid = request.p_bucket_uuid
        query = request.p_query

        try:
            if bucket_uuid not in self.buckets:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Bucket {bucket_uuid} not found.")
                return cloudberry_storage_pb2.FindResponse()

            logging.info(f"Searching in bucket {bucket_uuid} with query: {query}")

            response = cloudberry_storage_pb2.FindResponse()
            for content_uuid, entry in self.buckets[bucket_uuid].items():
                if query.lower() in entry['description'].lower():
                    response_entry = cloudberry_storage_pb2.FindResponseEntry(
                        p_content_uuid=content_uuid,
                        p_metrics=[
                            cloudberry_storage_pb2.Metric(
                                p_parameter=cloudberry_storage_pb2.Parameter.SEMANTIC_ONE_PEACE_SIMILARITY,
                                p_value=444.95),
                            cloudberry_storage_pb2.Metric(
                                p_parameter=cloudberry_storage_pb2.Parameter.RECOGNIZED_TEXT_SIMILARITY, p_value=0.45),
                            cloudberry_storage_pb2.Metric(
                                p_parameter=cloudberry_storage_pb2.Parameter.TEXTUAL_DESCRIPTION_SIMILARITY,
                                p_value=0.35),
                            cloudberry_storage_pb2.Metric(
                                p_parameter=cloudberry_storage_pb2.Parameter.RECOGNIZED_FACE_SIMILARITY, p_value=0.25),
                            cloudberry_storage_pb2.Metric(
                                p_parameter=cloudberry_storage_pb2.Parameter.RECOGNIZED_TEXT_BM25_RANK, p_value=0.15),
                            cloudberry_storage_pb2.Metric(
                                p_parameter=cloudberry_storage_pb2.Parameter.TEXTUAL_DESCRIPTION_BM25_RANK,
                                p_value=0.5),
                        ]
                    )
                    response.p_entries.append(response_entry)

            if not response.p_entries:
                logging.info(f"No results found for query: {query}")
            else:
                logging.info(f"Found {len(response.p_entries)} result(s) for query: {query}")

            return response
        except Exception as e:
            logging.error(f"Error searching in bucket {bucket_uuid} with query '{query}': {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("An internal error occurred.")
            return cloudberry_storage_pb2.FindResponse()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    cloudberry_storage_pb2_grpc.add_CloudberryStorageServicer_to_server(CloudberryStorageService(), server)
    server.add_insecure_port('[::]:50051')
    logging.info("Server started on port 50051")
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
