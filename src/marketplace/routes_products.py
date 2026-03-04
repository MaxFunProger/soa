from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from generated.models import (
    ProductCreate,
    ProductPage,
    ProductResponse,
    ProductStatus,
    ProductUpdate,
)
from src.marketplace.crud_products import (
    create_product,
    get_product,
    list_products,
    soft_delete_product,
    to_response,
    update_product,
)
from src.marketplace.database import get_db

router = APIRouter(prefix="/products", tags=["products"])


@router.post("", response_model=ProductResponse, status_code=201)
async def post_product(
    body: ProductCreate,
    db: AsyncSession = Depends(get_db),
) -> ProductResponse:
    return await create_product(db, body)


@router.get("", response_model=ProductPage)
async def get_products(
    page: int = Query(0, ge=0),
    size: int = Query(20, ge=1, le=100),
    status: ProductStatus | None = Query(None),
    category: str | None = Query(None, min_length=1, max_length=100),
    db: AsyncSession = Depends(get_db),
) -> ProductPage:
    content, total = await list_products(db, page=page, size=size, status=status, category=category)
    return ProductPage(
        content=content,
        totalElements=total,
        page=page,
        size=size,
    )


@router.get("/{id}", response_model=ProductResponse)
async def get_product_by_id(
    id: UUID,
    db: AsyncSession = Depends(get_db),
) -> ProductResponse:
    product = await get_product(db, id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return to_response(product)


@router.put("/{id}", response_model=ProductResponse)
async def put_product(
    id: UUID,
    body: ProductUpdate,
    db: AsyncSession = Depends(get_db),
) -> ProductResponse:
    product = await update_product(db, id, body)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return to_response(product)


@router.delete("/{id}", status_code=204)
async def delete_product(
    id: UUID,
    db: AsyncSession = Depends(get_db),
) -> None:
    ok = await soft_delete_product(db, id)
    if not ok:
        raise HTTPException(status_code=404, detail="Product not found")
