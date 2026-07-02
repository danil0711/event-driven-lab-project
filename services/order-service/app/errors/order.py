class OrderError(Exception):
    """Base order domain error"""
    pass


class ProductNotFoundError(OrderError):
    def __init__(self, product_id: int):
        self.product_id = product_id
        super().__init__(f"Продукт не найден по айди: {product_id}")