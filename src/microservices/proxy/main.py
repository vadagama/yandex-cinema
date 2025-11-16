"""
Proxy Service (API Gateway) для реализации паттерна Strangler Fig
Маршрутизирует запросы между монолитом и микросервисами с поддержкой постепенной миграции
"""
import os
import random
import logging
from fastapi import FastAPI, Request, Response, HTTPException
import httpx

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="CinemaAbyss Proxy Service",
    description="API Gateway с поддержкой паттерна Strangler Fig",
    version="1.0.0"
)

# Конфигурация из переменных окружения
PORT = int(os.getenv("PORT", "8000"))
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))

# HTTP клиент с таймаутами
http_client = httpx.AsyncClient(timeout=30.0)

logger.info(f"Proxy Service Configuration:")
logger.info(f"  PORT: {PORT}")
logger.info(f"  MONOLITH_URL: {MONOLITH_URL}")
logger.info(f"  MOVIES_SERVICE_URL: {MOVIES_SERVICE_URL}")
logger.info(f"  EVENTS_SERVICE_URL: {EVENTS_SERVICE_URL}")
logger.info(f"  GRADUAL_MIGRATION: {GRADUAL_MIGRATION}")
logger.info(f"  MOVIES_MIGRATION_PERCENT: {MOVIES_MIGRATION_PERCENT}%")


def should_route_to_microservice() -> bool:
    """
    Определяет, должен ли запрос быть направлен в микросервис на основе процента миграции.
    Использует случайное распределение для реализации паттерна Strangler Fig.
    """
    if not GRADUAL_MIGRATION:
        return False
    
    if MOVIES_MIGRATION_PERCENT <= 0:
        return False
    
    if MOVIES_MIGRATION_PERCENT >= 100:
        return True
    
    # Генерируем случайное число от 0 до 100
    random_value = random.randint(0, 100)
    return random_value < MOVIES_MIGRATION_PERCENT


async def proxy_request(
    target_url: str,
    request: Request,
    path_suffix: str = ""
) -> Response:
    """
    Проксирует HTTP запрос к целевому сервису.
    
    Args:
        target_url: Базовый URL целевого сервиса
        request: Входящий FastAPI Request
        path_suffix: Дополнительный путь, добавляемый к target_url
    
    Returns:
        Response от целевого сервиса
    """
    # Формируем полный URL
    full_url = f"{target_url}{path_suffix}"
    if request.url.query:
        full_url += f"?{request.url.query}"
    
    logger.info(f"Proxying {request.method} {request.url.path} -> {full_url}")
    
    # Получаем тело запроса
    body = await request.body()
    
    # Формируем заголовки (исключаем host и connection)
    headers = dict(request.headers)
    headers.pop("host", None)
    headers.pop("connection", None)
    
    try:
        # Выполняем запрос
        response = await http_client.request(
            method=request.method,
            url=full_url,
            headers=headers,
            content=body,
            follow_redirects=True
        )
        
        # Фильтруем заголовки ответа, исключая те, которые должны быть пересчитаны
        # или могут вызвать конфликты
        excluded_headers = {
            "content-length",  # Будет пересчитан FastAPI
            "transfer-encoding",  # Конфликтует с content-length
            "connection",  # Специфичен для соединения
            "keep-alive",  # Специфичен для соединения
            "upgrade",  # Специфичен для соединения
            "server",  # Информация о сервере
        }
        
        response_headers = {
            key: value
            for key, value in response.headers.items()
            if key.lower() not in excluded_headers
        }
        
        # Возвращаем ответ
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=response_headers,
            media_type=response.headers.get("content-type")
        )
    except httpx.RequestError as e:
        logger.error(f"Error proxying request to {full_url}: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"Service unavailable: {str(e)}"
        )


@app.get("/health")
async def health_check():
    """Health check endpoint для прокси-сервиса"""
    return {"status": "healthy", "message": "Strangler Fig Proxy is healthy"}


@app.api_route("/api/movies/health", methods=["GET"])
async def proxy_movies_health(request: Request):
    """Проксирует health check микросервиса фильмов"""
    return await proxy_request(MOVIES_SERVICE_URL, request, "/api/movies/health")


@app.api_route("/api/movies/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy_movies_with_path(request: Request, path: str):
    """
    Проксирует запросы к /api/movies/{path} с применением паттерна Strangler Fig.
    Распределяет трафик между монолитом и микросервисом на основе процента миграции.
    """
    # Определяем целевой сервис
    if should_route_to_microservice():
        target_url = MOVIES_SERVICE_URL
        logger.info(f"Routing to Movies Service (migration: {MOVIES_MIGRATION_PERCENT}%)")
    else:
        target_url = MONOLITH_URL
        logger.info(f"Routing to Monolith (migration: {MOVIES_MIGRATION_PERCENT}%)")
    
    # Формируем путь
    path_suffix = f"/api/movies/{path}"
    
    return await proxy_request(target_url, request, path_suffix)


@app.api_route("/api/movies", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy_movies(request: Request):
    """
    Проксирует запросы к /api/movies с применением паттерна Strangler Fig.
    Распределяет трафик между монолитом и микросервисом на основе процента миграции.
    """
    # Определяем целевой сервис
    if should_route_to_microservice():
        target_url = MOVIES_SERVICE_URL
        logger.info(f"Routing to Movies Service (migration: {MOVIES_MIGRATION_PERCENT}%)")
    else:
        target_url = MONOLITH_URL
        logger.info(f"Routing to Monolith (migration: {MOVIES_MIGRATION_PERCENT}%)")
    
    # Формируем путь
    path_suffix = "/api/movies"
    
    return await proxy_request(target_url, request, path_suffix)


@app.api_route("/api/events/health", methods=["GET"])
async def proxy_events_health(request: Request):
    """Проксирует health check микросервиса событий"""
    return await proxy_request(EVENTS_SERVICE_URL, request, "/api/events/health")


@app.api_route("/api/events/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy_events(request: Request, path: str):
    """
    Проксирует запросы к микросервису событий.
    """
    path_suffix = f"/api/events/{path}"
    return await proxy_request(EVENTS_SERVICE_URL, request, path_suffix)


@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy_monolith(request: Request, path: str):
    """
    Проксирует все остальные запросы к монолиту.
    Обрабатывает /api/users, /api/payments, /api/subscriptions и другие эндпоинты.
    """
    path_suffix = f"/api/{path}"
    return await proxy_request(MONOLITH_URL, request, path_suffix)


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy_root(request: Request, path: str):
    """
    Проксирует запросы к корневым путям (например, /health монолита).
    """
    path_suffix = f"/{path}"
    return await proxy_request(MONOLITH_URL, request, path_suffix)


@app.on_event("shutdown")
async def shutdown():
    """Закрываем HTTP клиент при завершении работы"""
    await http_client.aclose()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)

