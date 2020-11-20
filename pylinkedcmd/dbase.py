import os
from sqlalchemy import create_engine, MetaData, Table, Column, String, DateTime, Integer
from sqlalchemy.dialects.postgresql import JSONB, JSON
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker, synonym


class Pg:
    def __init__(self):
        cnx_string = "postgresql://%(PG_USER)s:%(PG_PASS)s@%(PG_HOST)s:%(PG_PORT)s/%(PG_DB)s" % os.environ
        self.db = create_engine(cnx_string)
        self.db_meta = MetaData(self.db)

        self.entities_table = Table(
            "entities",
            self.db_meta,
            Column("internal_id", Integer, primary_key=True, autoincrement=True),
            Column("entity_created", DateTime),
            Column("entity_source", String),
            Column("identifiers", JSONB),
            Column("name", String),
            Column("reference", String),
            Column("instance_of", String),
            Column("abstract", String),
            Column("date_published", Integer),
            Column("publisher", String),
            Column("string_representation", String),
            Column("alternate_name", String),
            Column("url", JSONB)
        )

        self.statements_table = Table(
            "statements",
            self.db_meta,
            Column("internal_id", Integer, primary_key=True, autoincrement=True),
            Column("claim_created", DateTime),
            Column("claim_source", String),
            Column("reference", String),
            Column("date_qualifier", DateTime),
            Column("subject_identifiers", JSONB),
            Column("subject_instance_of", String),
            Column("subject_label", String),
            Column("property_label", String),
            Column("object_identifiers", JSONB),
            Column("object_instance_of", String),
            Column("object_label", String),
            Column("object_qualifier", String)
        )

    def initialize(self):
        self.entities_table.create(bind=self.db, checkfirst=True)
        self.statements_table.create(bind=self.db, checkfirst=True)
