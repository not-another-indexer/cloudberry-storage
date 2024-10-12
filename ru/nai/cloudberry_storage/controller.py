from generated import cloudberry_storage_pb2_grpc as pb2_grpc
from generated import cloudberry_storage_pb2 as pb2


class CloudberryStorageService(pb2_grpc.CloudberryStorageServicer):
    def InitBucket(self, request, context):
        """
        Инициализирует корзину.

        Args:
            request (InitBucketRequest): Запрос на инициализацию корзины, содержащий UUID корзины.
                - bucket_uuid (string): UUID корзины, которую необходимо инициализировать.
            context (grpc.ServicerContext): Контекст запроса.

        Returns:
            InitBucketResponse: Ответ с сообщением о статусе и успехе операции.
                - status_message (string): Сообщение о статусе операции.
                - success (bool): Флаг успешности операции.
        """
        print(f"инициализация корзины с UUID: {request.bucket_uuid}")
        response = pb2.InitBucketResponse()

        if request.bucket_uuid:
            response.status_message = "корзина успешно инициализирована."
            response.success = True
        else:
            response.status_message = "не удалось инициализировать корзину."
            response.success = False

        return response

    def DestroyBucket(self, request, context):
        """
        Уничтожает корзину.

        Args:
            request (DestroyBucketRequest): Запрос на уничтожение корзины, содержащий UUID корзины.
                - bucket_uuid (string): UUID корзины, которую необходимо уничтожить.
            context (grpc.ServicerContext): Контекст запроса.

        Returns:
            DestroyBucketResponse: Ответ с сообщением о статусе и успехе операции.
                - status_message (string): Сообщение о статусе операции.
                - success (bool): Флаг успешности операции.
        """
        print(f"уничтожение корзины с UUID: {request.bucket_uuid}")
        response = pb2.DestroyBucketResponse()

        if request.bucket_uuid:
            response.status_message = "корзина успешно уничтожена."
            response.success = True
        else:
            response.status_message = "не удалось уничтожить корзину."
            response.success = False

        return response

    def PutEntry(self, request_iterator, context):
        """
        Добавляет записи в корзину.

        Args:
            request_iterator (Iterator[PutEntryRequest]): Итератор запросов на добавление записей, содержащий метаданные или данные чанков.
            context (grpc.ServicerContext): Контекст запроса.

        Returns:
            PutEntryResponse: Ответ с сообщением о статусе и успехе операции.
                - status_message (string): Сообщение о статусе операции.
                - success (bool): Флаг успешности операции.
        """
        response = pb2.PutEntryResponse()

        for request in request_iterator:
            if request.WhichOneof("payload") == "metadata":
                print(f"получены метаданные: {request.metadata.content_id}, {request.metadata.extension}")
            elif request.WhichOneof("payload") == "chunk_data":
                print(f"получены данные чанка размером: {len(request.chunk_data)} байт")

        response.status_message = "записи успешно добавлены."
        response.success = True

        return response

    def RemoveEntry(self, request, context):
        """
        Удаляет запись из корзины.

        Args:
            request (RemoveEntryRequest): Запрос на удаление записи, содержащий ID содержимого.
                - content_id (string): ID содержимого, которое необходимо удалить.
            context (grpc.ServicerContext): Контекст запроса.

        Returns:
            RemoveEntryResponse: Ответ с сообщением о статусе и успехе операции.
                - status_message (string): Сообщение о статусе операции.
                - success (bool): Флаг успешности операции.
        """
        print(f"удаление записи с ID содержимого: {request.content_id}")
        response = pb2.RemoveEntryResponse()

        if request.content_id:
            response.status_message = "запись успешно удалена."
            response.success = True
        else:
            response.status_message = "не удалось удалить запись."
            response.success = False

        return response