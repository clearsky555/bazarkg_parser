from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Text,
    Integer,
    select
)

from config import POSTGRES_URL

engine = create_engine(POSTGRES_URL)
meta = MetaData()


class BazarManager():

    def __init__(self, engine) -> None:
        self.engine = engine
        self.product = self.get_table_schema()

    def get_table_schema(self):
        product = Table(
            "products", meta,
            Column("id", Integer, primary_key=True),
            Column("title", String(200)),
            Column("som", String(200)),
            Column("dollar", String(200)),
            Column("mobile", String(200)),
            Column("city", String(200)),
            Column('link', String(255), nullable=False, unique=True)
        )
        return product

    def create_table(self):
        meta.create_all(self.engine, checkfirst=True)
        print("Таблица успешно создана")

    def insert_product(self, data):
        ins = self.product.insert().values(
            **data
        )
        connect = self.engine.connect()
        result = connect.execute(ins)
        connect.commit()

    def check_product_in_db(self, url):
        query = select(self.product).where(self.product.c.link == url)
        connect = self.engine.connect()
        result = connect.execute(query)
        result = result.fetchone()
        return result is not None