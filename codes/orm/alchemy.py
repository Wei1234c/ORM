from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker



class OrmClassBase:

    @classmethod
    def get_by_id(cls, id, session):
        return session.query(cls).get(id)


    @classmethod
    def to_dict(cls, key_field, value_field, session):
        return {getattr(row, key_field): getattr(row, value_field) for row in session.query(cls)}



class ModelBuilder:
    INDENT = '    '


    @classmethod
    def _class_name_from_table_name(cls, table_name):
        class_name = ''.join([part.capitalize() for part in table_name.split('_')])
        return class_name[:len(class_name) - 1] if class_name.endswith('s') else class_name


    @classmethod
    def _truncate_tables(cls, db_url):
        engine, meta, tables, session = cls.get_db_objects(db_url)

        for table in tables:
            session.execute(table.delete())

        session.commit()
        session.close()
        engine.dispose()


    @classmethod
    def get_tables(cls, db_url):
        _, _, tables, _ = cls.get_db_objects(db_url)
        return tables


    @classmethod
    def _gen_db_objects(cls, db_url):
        print()
        print('engine = create_engine("{}", echo = False)'.format(db_url.replace('\\', '\\\\')))
        print('meta = MetaData(bind = engine)')
        print('meta.reflect()')
        print('tables = [Table(table, meta) for table in meta.tables]')
        print('Session = sessionmaker(bind=engine)')
        print('session = Session()')
        print()


    @classmethod
    def _gen_imports(cls):
        print('from sqlalchemy import create_engine, MetaData, Table')
        print('from sqlalchemy.orm import mapper, sessionmaker')
        print()


    @classmethod
    def _gen_class_variables(cls, table):
        return []


    @classmethod
    def _gen_class_header(cls, table):
        class_name = cls._class_name_from_table_name(table.name)
        return 'class {}:\n'.format(class_name)


    @classmethod
    def _gen_fields_declaration(cls, table):
        fields = [column.name for column in table.columns if not column.primary_key]
        return cls.INDENT + 'def __init__(self, {}):\n'.format(', '.join(fields))


    @classmethod
    def _gen_fields_assignments(cls, table):
        return [cls.INDENT + cls.INDENT + 'self.{0} = {0}'.format(column.name)
                for column in table.columns if not column.primary_key]


    @classmethod
    def _gen_class_definition(cls, table):
        class_str = []

        class_str.append(cls._gen_class_header(table))
        class_str.extend(cls._gen_class_variables(table))
        class_str.append(cls._gen_fields_declaration(table))
        class_str.extend(cls._gen_fields_assignments(table))

        class_str = '\n'.join(class_str)

        print()
        print(class_str)
        print()


    @classmethod
    def _gen_classes_definitions(cls, db_url):
        tables = cls.get_tables(db_url)
        for t in tables:
            cls._gen_class_definition(t)


    @classmethod
    def _gen_mapping_strings(cls, db_url):
        tables = cls.get_tables(db_url)

        print()
        for i in range(len(tables)):
            print('{} = tables[{}]'.format(tables[i].name, i))

        print()
        for t in tables:
            print('mapper({}, {})'.format(cls._class_name_from_table_name(t.name), t.name))

        print()


    @classmethod
    def gen_all(cls, db_url, define_classes = True):
        if define_classes:
            cls._gen_classes_definitions(db_url)
        print('#*******************************************\n')

        cls._gen_imports()
        cls._gen_db_objects(db_url)
        cls._gen_mapping_strings(db_url)


    @classmethod
    def get_db_objects(cls, db_url, echo = False):
        engine = create_engine(db_url, echo = echo)
        meta = MetaData(bind = engine)
        meta.reflect()
        tables = [Table(table, meta) for table in meta.tables]
        session_maker = sessionmaker(bind = engine)
        session = session_maker()
        return engine, meta, tables, session
