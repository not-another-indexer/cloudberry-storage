import sys
import os
from concurrent import futures

import grpc
import pytesseract
import torch
import logging
from PIL import Image
from io import BytesIO
from sentence_transformers import SentenceTransformer
from torchvision import transforms
from qdrant_client import QdrantClient, models
from qdrant_client.grpc import PointStruct
from qdrant_client.http.models import Distance, VectorParams
from qdrant_client.http.exceptions import UnexpectedResponse
import numpy as np

# sys.path.append('generated')
import cloudberry_storage_pb2_grpc as pb2_grpc
import cloudberry_storage_pb2 as pb2

ONE_PEACE_GITHUB_REPO_DIR_PATH = 'ONE-PEACE/'
ONE_PEACE_MODEL_PATH = '/home/meno/models/one-peace.pt'
PYTESSERACT_PATH = r'/usr/bin/tesseract'

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class CloudberryStorageServicer(pb2_grpc.CloudberryStorageServicer):
    def __init__(self):
        self.client = QdrantClient("http://localhost:6333")
        # self.one_peace_model = self.init_one_peace_model()
        # self.text_model = self.init_sbert_model()
        self.transforms = transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ])

    def init_one_peace_model(self, model_dir=ONE_PEACE_GITHUB_REPO_DIR_PATH, model_name=ONE_PEACE_MODEL_PATH):
        if not os.path.isdir(model_dir):
            raise FileNotFoundError(f'The directory "{model_dir}" does not exist')
        if not os.path.isfile(model_name):
            raise FileNotFoundError(f'The model file "{model_name}" does not exist')
        one_peace_dir = os.path.normpath(ONE_PEACE_GITHUB_REPO_DIR_PATH)
        if not os.path.isdir(one_peace_dir):
            err_msg = f'The dir "{one_peace_dir}" does not exist'
            logger.error(err_msg)
            raise ValueError(err_msg)
        model_name = os.path.normpath(ONE_PEACE_MODEL_PATH)
        if not os.path.isfile(model_name):
            err_msg = f'The file "{model_name}" does not exist'
            logger.error(err_msg)
            raise ValueError(err_msg)
        sys.path.append(one_peace_dir)
        from one_peace.models import from_pretrained

        logger.info("Загрузка модели ONE-PEACE")
        current_workdir = os.getcwd()
        logger.info(f'Текущая рабочая директория: {current_workdir}')

        os.chdir(one_peace_dir)
        logger.info(f'Новая рабочая директория: {os.getcwd()}')
        model = from_pretrained(model_name, device=torch.device('cpu'))
        logger.info("ONE-PEACE был успешно загружен")
        return model

    def init_sbert_model(self):
        logger.info("Загрузка модели SBERT")
        model_sbert = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2', device='cpu')
        logger.info("SBERT был успешно загружен")
        return model_sbert

    def InitBucket(self, request, context):
        logger.info(f"Пришёл запрос с bucket_uuid: {request.p_bucket_uuid}.")
        p_bucket_uuid = request.p_bucket_uuid

        try:
            self.create_collection_if_not_exists(p_bucket_uuid)
            logger.info(f"Коллекция успешно проинициализирована.")
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error creating Qdrant collection: {e}")
            logger.error(f"Коллекция не создана из-за ошибки: {e}")
            return pb2.Empty()

        return pb2.Empty()

    def create_collection_if_not_exists(self, collection_name):
        try:
            self.client.get_collection(collection_name)
            logger.info(f"Коллекция {collection_name} уже существует.")
        except:
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config={
                    "one_peace_embedding": models.VectorParams(size=512, distance=Distance.COSINE),
                    "description_sbert_embedding": models.VectorParams(size=768, distance=Distance.COSINE),
                    "faces_text_sbert_embedding": models.VectorParams(size=128, distance=Distance.COSINE),
                    "ocr_text_sbert_embedding": models.VectorParams(size=256, distance=Distance.COSINE),
                }
            )
            logger.info(f"Создана новая коллекция {collection_name} с несколькими векторами.")

    def DestroyBucket(self, request, context):
        logger.info(f"Запрос на уничтожение коллекции с bucket_uuid: {request.p_bucket_uuid}.")
        p_bucket_uuid = request.p_bucket_uuid
        try:
            self.client.get_collection(p_bucket_uuid)
            logger.info(f"Коллекция успешно найдена.")
        except UnexpectedResponse:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Bucket collection {p_bucket_uuid} not found.")
            logger.error(f"Коллекция не найдена.")
            return pb2.Empty()

        try:
            self.client.delete_collection(p_bucket_uuid)
            print(f"Deleted collection for bucket: {p_bucket_uuid}")
            logger.info(f"Коллекция успешно удалена.")
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error deleting Qdrant collection: {e}")
            logger.error(f"Коллекция не уничтожена из-за ошибки: {e}.")
            return pb2.Empty()

        return pb2.Empty()

    def PutEntry(self, request, context):
        logger.info("Получено изображение.")
        try:
            bucket_uuid = request.p_metadata.p_bucket_uuid
            content_uuid = request.p_metadata.p_content_uuid
            description = request.p_metadata.p_description
            content_data = request.p_data
            image = Image.open(BytesIO(content_data)).convert("RGB")
            # image_vector = self.vectorize_image(image)
            image_vector = [np.random.rand(512).tolist() for _ in range(3)]
            # Получение OCR текста и вектора
            ocr_text = pytesseract.image_to_string(image, lang='eng+rus').strip()
            # ocr_vector = self.text_model.encode(ocr_text) if ocr_text else None
            ocr_vector = [np.random.rand(256).tolist() for _ in range(3)]
            logger.info(f"Распознанный текст OCR: {ocr_text}.")

            # Векторизация текстового описания
            # description_vector = self.text_model.encode(
            #     content_metadata.p_description) if content_metadata.p_description else None
            description_vector = [np.random.rand(768).tolist() for _ in range(3)]
            # Создание записи для Qdrant
            vectors = {
                "one_peace_embedding": image_vector,
                "description_sbert_embedding": description_vector,
                "faces_text_sbert_embedding": [np.random.rand(128).tolist() for _ in range(3)],
                "ocr_text_sbert_embedding": ocr_vector
            }
            self.client.upsert(
                collection_name=bucket_uuid,
                points=[
                    models.PointStruct(
                        id=content_uuid,
                        vector=vectors,
                        payload={
                            "description": description or "",
                            "ocr_text": ocr_text or ""
                        }
                    )
                ]
            )
            logger.info(f"Добавлена запись с ID {content_uuid} в коллекцию {bucket_uuid}.")

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Ошибка при добавлении записи: {e}")
            logger.info(f"Ошибка при добавлении записи: {e}.")
            return pb2.Empty()

        return pb2.Empty()

    def vectorize_image(self, image: Image.Image):
        """Векторизация изображения с помощью модели."""
        image = self.transforms(image).unsqueeze(0).to("cpu")
        with torch.no_grad():
            embedding = self.one_peace_model.extract_image_features(image).cpu().numpy()
        return embedding

    def RemoveEntry(self, request, context):
        bucket_uuid = request.p_bucket_uuid
        content_uuid = request.p_content_uuid
        if bucket_uuid in self.buckets and content_uuid in self.buckets[bucket_uuid]:
            del self.buckets[bucket_uuid][content_uuid]
            print(f"Removed entry {content_uuid} from bucket {bucket_uuid}")
            return pb2.Empty()
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Entry {content_uuid} in bucket {bucket_uuid} not found.")
            return pb2.Empty()

    def Find(self, request, context):
        query = request.p_query
        bucket_uuid = request.p_bucket_uuid
        parameters = request.p_parameters
        count = request.p_count or 10

        one_peace_vector = self.one_peace_model(query)
        sbert_vector = self.text_model(query)

        one_peace_results = self.search_in_qdrant(bucket_uuid, one_peace_vector, "one_peace_embedding", count)
        description_results = self.search_in_qdrant(bucket_uuid, sbert_vector, "description_sbert_embedding", count)
        ocr_results = self.search_in_qdrant(bucket_uuid, sbert_vector, "ocr_text_sbert_embedding", count)

        combined_results = self.combine_results(one_peace_results, description_results, ocr_results, parameters, count)

        response = pb2.FindResponse()
        for entry in combined_results:
            response.p_entries.add(
                content_uuid=entry['content_uuid'],
                metrics=entry['metrics']
            )

        if not response.p_entries:
            print(f"No results found for query: {query}")
        else:
            print(f"Found {len(response.p_entries)} result(s) for query: {query}")
        return response

    def search_in_qdrant(self, collection_name, query_vector, vector_name, top_k=10):
        result = self.client.query_points(
            collection_name=collection_name,
            query=query_vector,
            using=vector_name,
            limit=top_k,
        )
        return result

    def combine_results(self, one_peace_results, description_results, ocr_results, parameters, count):
        combined_results = {}

        for res in one_peace_results['result']:
            uuid = res['id']
            score = res['score']
            combined_results[uuid] = {
                'content_uuid': uuid,
                'metrics': [{'parameter': 'SEMANTIC_ONE_PEACE_SIMILARITY', 'value': score}]
            }

        for res in description_results['result']:
            uuid = res['id']
            score = res['score']
            if uuid in combined_results:
                combined_results[uuid]['metrics'].append(
                    {'parameter': 'TEXTUAL_DESCRIPTION_SIMILARITY', 'value': score})
            else:
                combined_results[uuid] = {
                    'content_uuid': uuid,
                    'metrics': [{'parameter': 'TEXTUAL_DESCRIPTION_SIMILARITY', 'value': score}]
                }

        for res in ocr_results['result']:
            uuid = res['id']
            score = res['score']
            if uuid in combined_results:
                combined_results[uuid]['metrics'].append({'parameter': 'RECOGNIZED_TEXT_SIMILARITY', 'value': score})
            else:
                combined_results[uuid] = {
                    'content_uuid': uuid,
                    'metrics': [{'parameter': 'RECOGNIZED_TEXT_SIMILARITY', 'value': score}]
                }

        for param in parameters:
            if param.parameter == pb2.Parameter.SEMANTIC_ONE_PEACE_SIMILARITY:
                for entry in combined_results.values():
                    entry['metrics'][0]['value'] *= param.value
            elif param.parameter == pb2.Parameter.TEXTUAL_DESCRIPTION_SIMILARITY:
                for entry in combined_results.values():
                    entry['metrics'][1]['value'] *= param.value

        sorted_results = sorted(combined_results.values(), key=lambda x: sum(m['value'] for m in x['metrics']),
                                reverse=True)

        return sorted_results[:count]


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_CloudberryStorageServicer_to_server(CloudberryStorageServicer(), server)
    server.add_insecure_port('[::]:8002')
    print("Server started on port 8002")
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
