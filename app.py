from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import logging
import os
import time
import uuid

APP_VERSION = os.getenv("APP_VERSION", "1.0.0")
APP_ENV = os.getenv("APP_ENV", "local")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger("mini-api")

app = FastAPI(title="Cloud Fluency Mini API")


@app.middleware("http")
async def add_request_logging(request: Request, call_next):
    request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
    start = time.time()

    try:
        response = await call_next(request)
    except Exception as e:
        logger.exception(
            "request_failed path=%s method=%s request_id=%s error=%s",
            request.url.path,
            request.method,
            request_id,
            str(e),
        )
        return JSONResponse(
            status_code=500,
            content={"error": "internal_server_error", "request_id": request_id},
        )

    duration_ms = round((time.time() - start) * 1000, 2)

    logger.info(
        "request_completed method=%s path=%s status=%s duration_ms=%s request_id=%s",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
        request_id,
    )

    response.headers["x-request-id"] = request_id
    return response


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ready")
def ready():
    return {
        "status": "ready",
        "env": APP_ENV,
        "version": APP_VERSION,
    }


@app.get("/version")
def version():
    return {
        "version": APP_VERSION,
        "env": APP_ENV,
    }


@app.get("/echo")
def echo(msg: str):
    return {"message": msg}


@app.get("/fail")
def fail():
    raise RuntimeError("simulated failure")