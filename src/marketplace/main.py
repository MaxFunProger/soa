from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.marketplace.routes_products import router as products_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(
    title="Marketplace Product API",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(products_router)
