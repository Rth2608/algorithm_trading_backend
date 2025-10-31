SQLAlchemy가 Class 정의를 읽고 DB에 실제 테이블이 어떻게 생겨야 하는지 파악

# 데이터 추가
SQL Query 대신 객체를 생성해서 session에 추가

# 데이터 조회
SQL Query 대신 Class를 이용해 조회

# ORM(Object Relational Mapper)
Declarative Core
1) DeclarativeBase

2) declarative_base

3) Mapped

4) mapped_column

5) relationship

6) registry


Session Management
1) Session

2) sessionmaker

3) scoped_session


Query & Loading Strategies
1) joinedload

2) subqueryload

3) selectinload

4) lazyload

5) noload


Query Utilities
1) aliased

2) contains_eager

3) with_parent

4) column_property

Low-Level & Legacy
1) Query

2) mapper

3) instrument_class