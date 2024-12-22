# AsyncJobStore

The Jobstore support async-engine of SQLAlchemy

## Introduction

The jobstore support async engine of SQLAlchemy.

If you are using APScheduler version 3.x in your project and have built SQLAlchemy with async-engine. you can use this library to directly add async-engine as the engine for APScheduler.

Tip: Support for async-engine has been added in APScheduler version 4.0 release. However, the 4.0 is still in pre-release.

## Sample

``` python
from sqlalchemy.ext.asyncio import create_async_engine
from async_jobstore import AsyncJobStore

async_engine = create_async_engine(f"postgresql+asyncpg://{user}:{pwd}@{host}:{port}/{db}")

jobstores = {
            "default": AsyncSQLAlchemyJobStore(engine=async_engine)
        }

executors = {
            "default": ThreadPoolExecutor(5),
        }

scheduler = AsyncIOScheduler(
    jobstores=jobstores,
    executors=executors
)
```
