import os
from sqlalchemy import create_engine, MetaData, Table, Column, String, DateTime, Integer
from sqlalchemy.dialects.postgresql import JSONB, JSON
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker


class Pg:
    def __init__(self):
        cnx_string = "postgresql://%(PG_USER)s:%(PG_PASS)s@%(PG_HOST)s:%(PG_PORT)s/%(PG_DB)s" % os.environ
        self.db = create_engine(cnx_string)
        self.db_meta = MetaData(self.db)

        self.entities_table = Table(
            "entities",
            self.db_meta,
            Column("identifiers", JSON),
            Column("entity_created", DateTime),
            Column("entity_source", String),
            Column("reference", String),
            Column("instance_of", String),
            Column("name", String),
            Column("abstract", String),
            Column("date_published", Integer),
            Column("publisher", String),
            Column("string_representation", String),
            Column("alternate_name", String),
            Column("url", JSON),
            Column("statements", JSON),
            Column("source_data", JSONB)
        )

        self.statements_table = Table(
            "statements",
            self.db_meta,
            Column("claim_created", DateTime),
            Column("claim_source", String),
            Column("reference", String),
            Column("subject_instance_of", String),
            Column("subject_label", String),
            Column("subject_identifiers", JSON),
            Column("object_identifier", String),
            Column("object_instance_of", String),
            Column("object_label", String),
            Column("object_qualifier", String),
            Column("property_label", String),
            Column("date_qualifier", DateTime),
            Column("object_identifiers", JSONB)
        )

    def initialize(self):
        self.entities_table.create(bind=self.db, checkfirst=True)
        self.statements_table.create(bind=self.db, checkfirst=True)

'''
class Entities(base):
    __tablename__ = 'entities'

    identifiers = Column("identifiers", JSON)
    entity_created = Column("entity_created", DateTime)
    entity_source = Column("entity_source", String)
    reference = Column("reference", String)
    instance_of = Column("instance_of", String)
    name = Column("name", String)
    abstract = Column("abstract", String)
    date_published = Column("date_published", Integer)
    publisher = Column("publisher", String)
    string_representation = Column("string_representation", String)
    alternate_name = Column("alternate_name", String)
    url = Column("url", JSON)
    statement = Column("statements", JSON)
    source_data = Column("source_data", JSONB)

class Statements(base):
    __tablename__ = 'statements'

    claim_created = Column("claim_created", DateTime)
    claim_source = Column("claim_source", String)
    reference = Column("reference", String)
    subject_instance_of = Column("subject_instance_of", String)
    subject_label = Column("subject_label", String)
    subject_identifier = Column("subject_identifiers", JSON)
    object_identifier = Column("object_identifier", String)
    object_identifiers = Column("object_identifiers", JSONB)
    object_instance_of = Column("object_instance_of", String)
    object_label = Column("object_label", String)
    object_qualifier = Column("object_qualifier", String)
    property_label = Column("property_label", String)
    date_qualifier = Column("date_qualifier", DateTime)
    object_identifiers = Column("object_identifiers", JSONB)

#Session = sessionmaker(db)  
#session = Session()

#base.metadata.create_all(db)
'''