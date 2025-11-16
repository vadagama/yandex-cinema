"""
Events Service - сервис для обработки событий через Kafka
Реализует producer и consumer для работы с событиями Movie, User, Payment
"""
import os
import json
import logging
import asyncio
import time
from datetime import datetime
from typing import Optional
from threading import Thread

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="CinemaAbyss Events Service",
    description="Сервис для обработки событий через Kafka",
    version="1.0.0"
)

# Конфигурация из переменных окружения
PORT = int(os.getenv("PORT", "8082"))
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")

# Топики Kafka
MOVIE_EVENTS_TOPIC = "movie-events"
USER_EVENTS_TOPIC = "user-events"
PAYMENT_EVENTS_TOPIC = "payment-events"

logger.info(f"Events Service Configuration:")
logger.info(f"  PORT: {PORT}")
logger.info(f"  KAFKA_BROKERS: {KAFKA_BROKERS}")


# Модели данных для событий
class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: int
    timestamp: Optional[str] = None


class UserEvent(BaseModel):
    user_id: int
    username: str
    action: str
    timestamp: Optional[str] = None


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: Optional[str] = None
    method_type: Optional[str] = None


# Kafka Producer
def create_producer():
    """Создает и возвращает Kafka Producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3
        )
        logger.info("Kafka Producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka Producer: {e}")
        raise


# Kafka Consumer
def create_consumer(topic: str, group_id: str):
    """Создает и возвращает Kafka Consumer для указанного топика"""
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        logger.info(f"Kafka Consumer created for topic: {topic}")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka Consumer for topic {topic}: {e}")
        raise


# Глобальный producer
producer = None


def init_producer():
    """Инициализирует глобальный producer"""
    global producer
    if producer is None:
        producer = create_producer()
    return producer


# Функция для обработки событий из Kafka
def process_movie_events():
    """Обрабатывает события из топика movie-events"""
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            consumer = create_consumer(MOVIE_EVENTS_TOPIC, "movie-events-consumer-group")
            logger.info(f"Starting consumer for topic: {MOVIE_EVENTS_TOPIC}")
            
            for message in consumer:
                event = message.value
                logger.info(f"[MOVIE EVENT] Received: {json.dumps(event, indent=2)}")
                logger.info(f"[MOVIE EVENT] Partition: {message.partition}, Offset: {message.offset}")
        except Exception as e:
            logger.warning(f"Error processing movie events (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to start movie events consumer after {max_retries} attempts")


def process_user_events():
    """Обрабатывает события из топика user-events"""
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            consumer = create_consumer(USER_EVENTS_TOPIC, "user-events-consumer-group")
            logger.info(f"Starting consumer for topic: {USER_EVENTS_TOPIC}")
            
            for message in consumer:
                event = message.value
                logger.info(f"[USER EVENT] Received: {json.dumps(event, indent=2)}")
                logger.info(f"[USER EVENT] Partition: {message.partition}, Offset: {message.offset}")
        except Exception as e:
            logger.warning(f"Error processing user events (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to start user events consumer after {max_retries} attempts")


def process_payment_events():
    """Обрабатывает события из топика payment-events"""
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            consumer = create_consumer(PAYMENT_EVENTS_TOPIC, "payment-events-consumer-group")
            logger.info(f"Starting consumer for topic: {PAYMENT_EVENTS_TOPIC}")
            
            for message in consumer:
                event = message.value
                logger.info(f"[PAYMENT EVENT] Received: {json.dumps(event, indent=2)}")
                logger.info(f"[PAYMENT EVENT] Partition: {message.partition}, Offset: {message.offset}")
        except Exception as e:
            logger.warning(f"Error processing payment events (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to start payment events consumer after {max_retries} attempts")


# Запуск consumers в отдельных потоках
def start_consumers():
    """Запускает consumers в отдельных потоках"""
    movie_thread = Thread(target=process_movie_events, daemon=True)
    user_thread = Thread(target=process_user_events, daemon=True)
    payment_thread = Thread(target=process_payment_events, daemon=True)
    
    movie_thread.start()
    user_thread.start()
    payment_thread.start()
    
    logger.info("All Kafka consumers started")


# Инициализация при старте приложения
@app.on_event("startup")
async def startup_event():
    """Инициализация при старте приложения"""
    logger.info("Starting Events Service...")
    
    # Инициализируем producer с retry логикой
    max_retries = 10
    retry_delay = 3
    
    for attempt in range(max_retries):
        try:
            init_producer()
            logger.info("Kafka Producer initialized")
            break
        except Exception as e:
            logger.warning(f"Failed to initialize Kafka Producer (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"Failed to initialize Kafka Producer after {max_retries} attempts")
    
    # Запускаем consumers
    try:
        # Задержка для подключения к Kafka
        await asyncio.sleep(5)
        start_consumers()
        logger.info("Kafka Consumers started")
    except Exception as e:
        logger.error(f"Failed to start Kafka Consumers: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Очистка ресурсов при завершении работы"""
    global producer
    if producer:
        producer.close()
        logger.info("Kafka Producer closed")


# API Endpoints
@app.get("/api/events/health")
async def health_check():
    """Health check endpoint"""
    return {"status": True, "message": "Events Service is healthy"}


@app.post("/api/events/movie", status_code=201)
async def create_movie_event(event: MovieEvent):
    """
    Создает событие фильма и публикует его в Kafka
    """
    try:
        # Добавляем timestamp, если не указан
        event_data = event.model_dump()
        if not event_data.get("timestamp"):
            event_data["timestamp"] = datetime.utcnow().isoformat()
        
        # Публикуем событие в Kafka
        producer = init_producer()
        future = producer.send(
            MOVIE_EVENTS_TOPIC,
            key=str(event_data["movie_id"]),
            value=event_data
        )
        
        # Ждем подтверждения
        record_metadata = future.get(timeout=10)
        
        logger.info(f"Movie event published to topic {MOVIE_EVENTS_TOPIC}, "
                   f"partition {record_metadata.partition}, offset {record_metadata.offset}")
        
        return {
            "status": "success",
            "message": "Movie event created and published",
            "event": event_data,
            "kafka_info": {
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset
            }
        }
    except KafkaError as e:
        logger.error(f"Kafka error while publishing movie event: {e}")
        raise HTTPException(status_code=503, detail=f"Failed to publish event to Kafka: {str(e)}")
    except Exception as e:
        logger.error(f"Error creating movie event: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/api/events/user", status_code=201)
async def create_user_event(event: UserEvent):
    """
    Создает событие пользователя и публикует его в Kafka
    """
    try:
        # Добавляем timestamp, если не указан
        event_data = event.model_dump()
        if not event_data.get("timestamp"):
            event_data["timestamp"] = datetime.utcnow().isoformat()
        
        # Публикуем событие в Kafka
        producer = init_producer()
        future = producer.send(
            USER_EVENTS_TOPIC,
            key=str(event_data["user_id"]),
            value=event_data
        )
        
        # Ждем подтверждения
        record_metadata = future.get(timeout=10)
        
        logger.info(f"User event published to topic {USER_EVENTS_TOPIC}, "
                   f"partition {record_metadata.partition}, offset {record_metadata.offset}")
        
        return {
            "status": "success",
            "message": "User event created and published",
            "event": event_data,
            "kafka_info": {
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset
            }
        }
    except KafkaError as e:
        logger.error(f"Kafka error while publishing user event: {e}")
        raise HTTPException(status_code=503, detail=f"Failed to publish event to Kafka: {str(e)}")
    except Exception as e:
        logger.error(f"Error creating user event: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/api/events/payment", status_code=201)
async def create_payment_event(event: PaymentEvent):
    """
    Создает событие платежа и публикует его в Kafka
    """
    try:
        # Добавляем timestamp, если не указан
        event_data = event.model_dump()
        if not event_data.get("timestamp"):
            event_data["timestamp"] = datetime.utcnow().isoformat()
        
        # Публикуем событие в Kafka
        producer = init_producer()
        future = producer.send(
            PAYMENT_EVENTS_TOPIC,
            key=str(event_data["payment_id"]),
            value=event_data
        )
        
        # Ждем подтверждения
        record_metadata = future.get(timeout=10)
        
        logger.info(f"Payment event published to topic {PAYMENT_EVENTS_TOPIC}, "
                   f"partition {record_metadata.partition}, offset {record_metadata.offset}")
        
        return {
            "status": "success",
            "message": "Payment event created and published",
            "event": event_data,
            "kafka_info": {
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset
            }
        }
    except KafkaError as e:
        logger.error(f"Kafka error while publishing payment event: {e}")
        raise HTTPException(status_code=503, detail=f"Failed to publish event to Kafka: {str(e)}")
    except Exception as e:
        logger.error(f"Error creating payment event: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)

