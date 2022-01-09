import abc
import model


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError


class SqlRepository(AbstractRepository):
    def __init__(self, session):
        self.session = session

    def add(self, batch: model.Batch):
        self.session.execute(
            """
        INSERT INTO batches (reference, sku, _purchased_quantity, eta)
        values (:ref, :sku, :qty, :eta)
        """,
            {
                "ref": batch.reference,
                "sku": batch.sku,
                "qty": batch._purchased_quantity,
                "eta": batch.eta,
            },
        )

    def get(self, reference: str) -> model.Batch:
        batch_id, *res = self.session.execute(
            """
        SELECT * from batches
        where reference = :ref
        """,
            {'ref': reference},
        ).one()
        batch = model.Batch(*res)
        _, *order_res = self.session.execute(
            """
        select * from order_lines
        where sku = :sku
        """,
            {'sku': batch.sku},
        )
        print(order_res)
        return model.Batch(*res)
