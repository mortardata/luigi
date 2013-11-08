# Copyright (c) 2013 Mortar Data Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

from luigi.scheduler_engine import scheduler_engine

from pymongo import MongoClient

class MongoSchedulerEngine(scheduler_engine.SchedulerEngine):
    """
    """
    def __init__(self, mongo_uri, workers_collection='luigi_workers', tasks_collection='luigi_tasks'):
        self.client = MongoClient(mongo_uri)
        self.db = self.client.get_default_database()
        self.workers_collection = self.db[workers_collection]
        self.tasks_collection = self.db[tasks_collection]
    
    def load(self):
        self.workers_collection.ensure_index('worker_id', unique=True)
        self.tasks_collection.ensure_index('task_id', unique=True)

    def dump(self):
        self.client.close()

    def get_workers(self):
        return dict([(w['worker_id'], w['last_update_timestamp']) \
                     for w in self.workers_collection.find()])

    def set_worker(self, worker, timestamp):
        self.workers_collection.update({'worker_id': worker},
            {'$set': {'last_update_timestamp': timestamp}})

    def delete_workers(self, workers):
        if workers:
            self.workers_collection.remove({'worker_id': {'$in': workers}})

    def get_task(self, task_id):
        db_task = self.tasks_collection.find_one({'task_id': task_id})
        return self._db_task_to_scheduler_task(db_task) if db_task else None

    def get_tasks(self):
        db_tasks = self.tasks_collection.find()
        return dict([(db_task['task_id'], self._db_task_to_scheduler_task(db_task)) \
                    for db_task in db_tasks])

    def find_or_add_task(self, task_id, task):
        # TODO make this use fancy find_or_modify for 2.4 and fallback to insert-catch-find for 2.2
        lookup_task = self.get_task(task_id)
        if lookup_task:
            return lookup_task
        else:
            db_task = self._scheduler_task_to_db_task(task_id, task)
            self.tasks_collection.insert(db_task)
            return task

    def update_task(self, task_id, task):
        db_task = self._scheduler_task_to_db_task(task_id, task)
        self.tasks_collection.update({'task_id': task_id}, db_task)

    def delete_tasks(self, task_ids):
        if task_ids:
            self.tasks_collection.remove({'task_id': {'$in': task_ids}})

    def _db_task_to_scheduler_task(self, db_task):
        return scheduler_engine.Task(db_task.get('status'), 
                    db_task.get('deps'), 
                    db_task.get('stakeholders'), 
                    db_task.get('workers'), 
                    db_task.get('time'),
                    db_task.get('retry'),
                    db_task.get('remove'),
                    db_task.get('worker_running'),
                    db_task.get('expl'))

    def _scheduler_task_to_db_task(self, task_id, task):
        return {'task_id': task_id,
                'status': task.status,
                'deps': list(task.deps),
                'stakeholders': list(task.stakeholders),
                'workers': list(task.workers),
                'time': task.time,
                'retry': task.retry,
                'remove': task.remove,
                'worker_running': task.worker_running,
                'expl': task.expl}
