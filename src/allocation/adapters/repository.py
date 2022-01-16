import abc
from allocation.domain import model


class AbstractProductRepository(abc.ABC):
    def add(self, product: model.Product):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Product:
        raise NotImplementedError


class SqlAlchemyRepository(AbstractProductRepository):
    def __init__(self, session):
        self.session = session

    def add(self, batch):
        self.session.add(batch)

    def get(self, sku):
        return self.session.query(model.Product).filter_by(sku=sku).one()

    def list(self):
        return self.session.query(model.Product).all()
