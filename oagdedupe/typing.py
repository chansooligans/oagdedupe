import sqlalchemy


class SESSION(sqlalchemy.orm.session.Session):
    pass


class TABLE(sqlalchemy.orm.decl_api.DeclarativeMeta):
    pass


class SUBQUERY(sqlalchemy.sql.selectable.Subquery):
    pass
