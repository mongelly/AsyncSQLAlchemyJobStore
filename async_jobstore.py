from __future__ import absolute_import

import asyncio
from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.util import maybe_ref, datetime_to_utc_timestamp, utc_timestamp_to_datetime
from apscheduler.job import Job

try:
    import cPickle as pickle # type: ignore
except ImportError:  # pragma: nocover
    import pickle

try:
    from sqlalchemy import (
        Table, Column, MetaData, Unicode, Float, LargeBinary, select, and_)
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.sql.expression import null
except ImportError:  # pragma: nocover
    raise ImportError('AsyncSQLAlchemyJobStore requires SQLAlchemy and SQLAlchemy-Asyncio extras installed')

class AsyncJobStore(BaseJobStore):
    def __init__(self, url=None, engine=None, tablename='apscheduler_jobs', metadata=None,
                 pickle_protocol=pickle.HIGHEST_PROTOCOL, tableschema=None, engine_options=None):
        super(AsyncJobStore, self).__init__()
        self.pickle_protocol = pickle_protocol
        metadata = maybe_ref(metadata) or MetaData()

        if engine:
            self.engine = maybe_ref(engine)
            if self.engine.driver != "asyncpg":
                raise ValueError('The "engine" must be sqlalchemy async engine ')
            
        elif url:
            if "asyncpg" in url:
                self.engine = create_async_engine(url, **(engine_options or {}))
            else:
                raise ValueError('The "url" must be an asyncio-compatible url')
        else:
            raise ValueError('Need either "engine" or "url" defined')

        # 191 = max key length in MySQL for InnoDB/utf8mb4 tables,
        # 25 = precision that translates to an 8-byte float
        self.jobs_t = Table(
            tablename, metadata,
            Column('id', Unicode(191), primary_key=True),
            Column('next_run_time', Float(25), index=True),
            Column('job_state', LargeBinary, nullable=False),
            schema=tableschema
        )
        
        self._loop = asyncio.get_event_loop()
        
    def start(self, scheduler, alias):
        super(AsyncJobStore, self).start(scheduler, alias)
        self._loop.run_until_complete(self._async_create_table())
    
    async def _async_create_table(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(lambda conn: self.jobs_t.create(conn, checkfirst=True))
            
    def shutdown(self):
        self._loop.run_until_complete(self.engine.dispose())       
        
    def lookup_job(self, job_id):
        return self._loop.run_until_complete(self._async_lookup_job(job_id))
    
    async def _async_lookup_job(self,job_id):
        async with self.engine.begin() as conn:
            stmt = select(self.jobs_t.c.job_state).\
                where(self.jobs_t.c.id == job_id)
            job_state = await conn.execute(stmt)
            return self._reconstitute_job(job_state) if job_state else None
                     
    def get_due_jobs(self, now):
        return self._loop.run_until_complete(self._async_get_due_jobs(now))
    
    async def _async_get_due_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        return await self._get_jobs(self.jobs_t.c.next_run_time <= timestamp)
    
    
    def get_next_run_time(self):
        return self._loop.run_until_complete(self._async_get_next_run_time())
    
    async def _async_get_next_run_time(self):
        async with self.engine.begin() as conn:
            stmt = select(self.jobs_t.c.next_run_time).\
                where(self.jobs_t.c.next_run_time != null()).\
                order_by(self.jobs_t.c.next_run_time).limit(1)
            next_run_time = (await conn.execute(stmt)).scalar()
            return utc_timestamp_to_datetime(next_run_time)
    
    
    def get_all_jobs(self):
        return self._loop.run_until_complete(self._async_get_all_jobs())
    
    async def _async_get_all_jobs(self):
        jobs = await self._get_jobs()
        self._fix_paused_jobs_sorting(jobs=jobs)
        return jobs
      
    def add_job(self, job):
        self._loop.run_until_complete(self._async_add_job(job))
    
    async def _async_add_job(self,job):
        insert = self.jobs_t.insert().values(**{
                "id":job.id,
                "next_run_time":datetime_to_utc_timestamp(job.next_run_time),
                "job_state":pickle.dumps(job.__getstate__(), self.pickle_protocol)
            })
        
        try:
            async with self.engine.begin() as conn:
                await conn.execute(insert)
        except IntegrityError:
            raise ConflictingIdError(job.id)
    

    def update_job(self, job):
        self._loop.run_until_complete(self._async_update_job(job))
    
    async def _async_update_job(self,job):
        update = self.jobs_t.update(**{
            'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
            'job_state': pickle.dumps(job.__getstate__(), self.pickle_protocol)
        }).where(self.jobs_t.c.id == job.id)
        
        async with self.engine.begin() as conn:
                result = await conn.execute(update)
                if result.rowcount == 0:
                    raise JobLookupError(job.id)
    

    def remove_job(self, job_id):
        self._loop.run_until_complete(self._async_remove_job(job_id))
    
    async def _async_remove_job(self,job_id):
        delete = self.jobs_t.delete().where(self.jobs_t.c.id == job_id)
        async with self.engine.begin() as conn:
            result = await conn.execute(delete)
            if result.rowcount == 0:
                    raise JobLookupError(job_id)
    

    def remove_all_jobs(self):
        self._loop.run_until_complete(self._async_remove_all_jobs())
    
    async def _async_remove_all_jobs(self):
        async with self.engine.begin() as conn:
            await conn.execute(self.jobs_t.delete())
    
    def _reconstitute_job(self, job_state):
        job_state = pickle.loads(job_state)
        job_state['jobstore'] = self
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        return job
    
    async def _get_jobs(self, *conditions):
        jobs = []
        selectable = select(self.jobs_t.c.id, self.jobs_t.c.job_state).order_by(self.jobs_t.c.next_run_time)
        stmt = selectable.where(and_(*conditions)) if conditions else selectable
        failed_job_ids = set()
        async with self.engine.begin() as conn:
            for row in await conn.execute(stmt):
                try:
                    jobs.append(self._reconstitute_job(row.job_state))
                except BaseException:
                    self._logger.exception('Unable to restore job "%s" -- removing it', row.id)
                    failed_job_ids.add(row.id)
            if failed_job_ids:
                delete = self.jobs_t.delete().where(self.jobs_t.c.id.in_(failed_job_ids))
                await conn.execute(delete)
        return jobs
    
    def _fix_paused_jobs_sorting(self, jobs):
        for i, job in enumerate(jobs):
            if job.next_run_time is not None:
                if i > 0:
                    paused_jobs = jobs[:i]
                    del jobs[:i]
                    jobs.extend(paused_jobs)
                break