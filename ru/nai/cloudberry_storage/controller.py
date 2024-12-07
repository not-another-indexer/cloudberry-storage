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
from qdrant_client.models import Distance, VectorParams
from qdrant_client.http.exceptions import UnexpectedResponse
import numpy as np

# sys.path.append('generated')
import cloudberry_storage_pb2_grpc as pb2_grpc
import cloudberry_storage_pb2 as pb2
from cloudberry_storage_pb2 import Coefficient

ONE_PEACE_GITHUB_REPO_DIR_PATH = 'ONE-PEACE/'
ONE_PEACE_MODEL_PATH = '/home/meno/models/one-peace.pt'
PYTESSERACT_PATH = r'/usr/bin/tesseract'
ONE_PEACE_VECTOR_SIZE = 1536
SBERT_VECTOR_SIZE = 384

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
        self.one_peace_model = self.init_one_peace_model()
        self.text_model = self.init_sbert_model()
        self.transforms = transforms.Compose([
            transforms.Resize((256, 256)),
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
        logger.info(f"Пришёл запрос на инициализацию bucket с UUID: {request.p_bucket_uuid}.")
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
                    "one_peace_embedding": models.VectorParams(size=ONE_PEACE_VECTOR_SIZE, distance=Distance.COSINE),
                    "description_sbert_embedding": models.VectorParams(size=SBERT_VECTOR_SIZE,
                                                                       distance=Distance.COSINE),
                    "faces_text_sbert_embedding": models.VectorParams(size=SBERT_VECTOR_SIZE, distance=Distance.COSINE),
                    "ocr_text_sbert_embedding": models.VectorParams(size=SBERT_VECTOR_SIZE, distance=Distance.COSINE),
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
            logger.info(
                f"Метаданные: bucket_uuid={bucket_uuid}, content_uuid={content_uuid}, description={description}")

            image = Image.open(BytesIO(content_data)).convert("RGB")
            image_vector = self.vectorize_image(image).tolist()
            # image_vector = np.random.rand(ONE_PEACE_VECTOR_SIZE).tolist()
            logger.info(f"Размер вектора изображения: {len(image_vector)}")

            # Получение OCR текста и вектора
            ocr_text = pytesseract.image_to_string(image, lang='eng+rus').strip()
            ocr_vector = self.text_model.encode(ocr_text).tolist() if ocr_text else np.zeros(SBERT_VECTOR_SIZE).tolist()
            # ocr_vector = np.random.rand(SBERT_VECTOR_SIZE).tolist()
            logger.info(f"OCR текст: {ocr_text}, размер OCR вектора: {len(ocr_vector) if ocr_vector else 'None'}")

            # Векторизация текстового описания
            description_vector = self.text_model.encode(description).tolist() if description else np.zeros(
                SBERT_VECTOR_SIZE).tolist()
            # description_vector = np.random.rand(SBERT_VECTOR_SIZE).tolist()
            logger.info(f"Размер вектора описания: {len(description_vector) if description_vector else 'None'}")

            # Создание записи для Qdrant
            vectors = {
                "one_peace_embedding": image_vector,
                "description_sbert_embedding": description_vector,
                "faces_text_sbert_embedding": np.zeros(SBERT_VECTOR_SIZE).tolist(),
                "ocr_text_sbert_embedding": ocr_vector
            }

            # self.validate_vector_sizes(vectors)

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
            logger.info(f"Запись с ID {content_uuid} добавлена в коллекцию {bucket_uuid}.")

        except Exception as e:
            logger.error(f"Ошибка при добавлении записи: {e}.")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Ошибка при добавлении записи: {e}")
            return pb2.Empty()

        return pb2.Empty()

    # def validate_vector_sizes(self, vectors: dict):
    #     """Проверка размеров векторов."""
    #     logger.info("Проверка размеров векторов.")
    #     expected_sizes = {
    #         "one_peace_embedding": ONE_PEACE_VECTOR_SIZE,
    #         "description_sbert_embedding": SBERT_VECTOR_SIZE,
    #         "faces_text_sbert_embedding": SBERT_VECTOR_SIZE,
    #         "ocr_text_sbert_embedding": SBERT_VECTOR_SIZE
    #     }
    #     for name, vector in vectors.items():
    #         if vector is not None and len(vector) != expected_sizes[name]:
    #             raise ValueError(
    #                 f"Размер вектора '{name}' ({len(vector)}) не соответствует ожидаемому ({expected_sizes[name]})")

    def vectorize_image(self, image: Image.Image):
        """Векторизация изображения с помощью модели."""
        logger.info("Начало векторизации изображения.")
        try:
            image_tensor = self.transforms(image).unsqueeze(0).to("cpu")
            logger.info(f"Размер изображения после преобразований: {image_tensor.shape}")
            with torch.no_grad():
                embedding = self.one_peace_model.extract_image_features(image_tensor).cpu().numpy()
            logger.info(f"Shape вектора изображения: {embedding.shape}")

            expected_size = ONE_PEACE_VECTOR_SIZE
            if embedding.shape[-1] != expected_size:
                raise ValueError(f"Размер вектора изображения {embedding.shape[-1]}, ожидается {expected_size}")

            return embedding.flatten()
        except Exception as e:
            logger.error(f"Ошибка векторизации изображения: {e}", exc_info=True)
            raise

    def RemoveEntry(self, request, context):
        """Удаление записи из Qdrant."""
        bucket_uuid = request.p_bucket_uuid
        content_uuid = request.p_content_uuid

        logger.info(f"Запрос на удаление записи с ID {content_uuid} из коллекции {bucket_uuid}.")
        try:
            # Проверка существования коллекции
            self.client.get_collection(bucket_uuid)
            logger.info(f"Коллекция {bucket_uuid} найдена.")
        except UnexpectedResponse:
            logger.error(f"Коллекция {bucket_uuid} не найдена.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Коллекция с bucket_uuid {bucket_uuid} не найдена.")
            return pb2.Empty()

        try:
            # Удаление точки по её ID
            self.client.delete(
                collection_name=bucket_uuid,
                points_selector=models.PointIdsList(point_ids=[content_uuid])
            )
            logger.info(f"Запись с ID {content_uuid} успешно удалена из коллекции {bucket_uuid}.")
        except Exception as e:
            logger.error(f"Ошибка при удалении записи: {e}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Ошибка при удалении записи: {e}")
            return pb2.Empty()

        return pb2.Empty()

    def Find(self, request, context):
        logger.info(f"Search request with query: {request.p_query}.")
        query = request.p_query
        bucket_uuid = request.p_bucket_uuid
        parameters = request.p_parameters
        count = request.p_count or 10

        text_tokens = self.one_peace_model.process_text([query])
        with torch.no_grad():
            one_peace_vector = self.one_peace_model.extract_text_features(text_tokens).cpu().numpy().flatten().tolist()
        # one_peace_vector = np.random.rand(ONE_PEACE_VECTOR_SIZE).tolist()
        sbert_vector = self.text_model.encode(query).tolist()
        # sbert_vector = np.random.rand(SBERT_VECTOR_SIZE).tolist()

        one_peace_results = self.search_in_qdrant(bucket_uuid, one_peace_vector, "one_peace_embedding", count)
        description_results = self.search_in_qdrant(bucket_uuid, sbert_vector, "description_sbert_embedding", count)
        ocr_results = self.search_in_qdrant(bucket_uuid, sbert_vector, "ocr_text_sbert_embedding", count)

        combined_results = self.combine_results(one_peace_results, description_results, ocr_results, parameters, count)

        response = pb2.FindResponse()
        for entry in combined_results:
            response.p_entries.add(
                p_content_uuid=entry['p_content_uuid'],
                p_metrics=entry['p_metrics']
            )

        if not response.p_entries:
            logger.info(f"No results found for query: {query}")
        else:
            logger.info(f"Found {len(response.p_entries)} result(s) for query: {query}")
        return response

    def search_in_qdrant(self, collection_name, query_vector, vector_name, top_k=10):
        result = self.client.query_points(
            collection_name=collection_name,
            query=query_vector,
            using=vector_name,
            limit=top_k,
        )
        return result.points

    def combine_results(self, one_peace_results, description_results, ocr_results, parameters, count):
        combined_results = {}
        logger.info(f"Parameters: {parameters}")
        logger.info(f"Wanted count: {count}")
        logger.info(f"One-peace results: {one_peace_results}")
        logger.info(f"Description similarity results: {description_results}")
        logger.info(f"Ocr similarity results: {ocr_results}")

        # def normalize_results(results):
        #     """Нормирует список результатов по значениям score."""
        #     if not results:
        #         return results
        #     scores = [res.score for res in results]
        #     min_score = min(scores)
        #     max_score = max(scores)
        #     if max_score == min_score:
        #         # Если все значения одинаковые, установить все нормализованные значения в 0
        #         return [type(res)(id=res.id, score=1) for res in results]
        #     return [type(res)(id=res.id, score=(res.score - min_score) / (max_score - min_score)) for res in results]

        # one_peace_results = normalize_results(one_peace_results)
        # description_results = normalize_results(description_results)
        # ocr_results = normalize_results(ocr_results)

        for source_results, parameter_name in [
            (one_peace_results, 'SEMANTIC_ONE_PEACE_SIMILARITY'),
            (description_results, 'TEXTUAL_DESCRIPTION_SIMILARITY'),
            (ocr_results, 'RECOGNIZED_TEXT_SIMILARITY'),
        ]:
            for res in source_results:
                uuid = res.id
                score = res.score
                if uuid not in combined_results:
                    combined_results[uuid] = {
                        'p_content_uuid': uuid,
                        'p_metrics': [],
                    }
                combined_results[uuid]['p_metrics'].append({'p_parameter': parameter_name, 'p_value': score})

        logger.info(f"Combined results before reranking: {combined_results}")
        # Применяем коэффициенты к каждому параметру
        parameter_weights = {str(param.p_parameter): param.p_value for param in parameters}
        for entry in combined_results.values():
            # Применяем веса к каждой метрике
            for metric in entry['p_metrics']:
                metric['p_value'] *= parameter_weights.get(metric['p_parameter'], 0)
        logger.info(f"Combined results: {combined_results}")
        # Ранжируем результаты по итоговой сумме метрик
        sorted_results = sorted(
            combined_results.values(),
            key=lambda x: sum(m['p_value'] for m in x['p_metrics']),
            reverse=True
        )
        logger.info(f"Sorted results: {sorted_results[:count]}")
        # Возвращаем только топ `count` результатов
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
