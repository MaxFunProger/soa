from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from generated.models import ProductCreate, ProductResponse, ProductStatus, ProductUpdate
from src.marketplace.models_db import Product


def to_response(p: Product) -> ProductResponse:
    return ProductResponse(
        id=p.id,
        name=p.name,
        description=p.description,
        price=float(p.price),
        stock=p.stock,
        category=p.category,
        status=ProductStatus(p.status),
        created_at=p.created_at,
        updated_at=p.updated_at,
    )


async def create_product(session: AsyncSession, body: ProductCreate) -> ProductResponse:
    product = Product(
        name=body.name,
        description=body.description,
        price=body.price,
        stock=body.stock,
        category=body.category,
        status=body.status.value,
    )
    session.add(product)
    await session.flush()
    await session.refresh(product)
    return to_response(product)


async def get_product(session: AsyncSession, id: UUID) -> Product | None:
    result = await session.execute(select(Product).where(Product.id == id))
    return result.scalar_one_or_none()


async def list_products(
    session: AsyncSession,
    page: int = 0,
    size: int = 20,
    status: ProductStatus | None = None,
    category: str | None = None,
) -> tuple[list[ProductResponse], int]:
    q = select(Product)
    count_q = select(func.count()).select_from(Product)
    if status is not None:
        q = q.where(Product.status == status.value)
        count_q = count_q.where(Product.status == status.value)
    if category is not None:
        q = q.where(Product.category == category)
        count_q = count_q.where(Product.category == category)
    total = (await session.execute(count_q)).scalar_one()
    q = q.offset(page * size).limit(size)
    result = await session.execute(q)
    products = result.scalars().all()
    return [to_response(p) for p in products], total


async def update_product(
    session: AsyncSession, id: UUID, body: ProductUpdate
) -> Product | None:
    product = await get_product(session, id)
    if product is None:
        return None
    if body.name is not None:
        product.name = body.name
    if body.description is not None:
        product.description = body.description
    if body.price is not None:
        product.price = body.price
    if body.stock is not None:
        product.stock = body.stock
    if body.category is not None:
        product.category = body.category
    if body.status is not None:
        product.status = body.status.value
    await session.flush()
    await session.refresh(product)
    return product


async def soft_delete_product(session: AsyncSession, id: UUID) -> bool:
    product = await get_product(session, id)
    if product is None:
        return False
    product.status = ProductStatus.ARCHIVED.value
    await session.flush()
    return True
