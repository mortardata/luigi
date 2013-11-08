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

import cPickle as pickle
import logging
import os

from luigi.scheduler_engine import scheduler_engine

logger = logging.getLogger("luigi.server")

class PickleSchedulerEngine(scheduler_engine.SchedulerEngine):

    def __init__(self, state_path):
        self._state_path = state_path
        self._tasks = {} # dict from task_id to Task
        self._workers = {}  # dict from id to timestamp (last updated)

    def load(self):
        if os.path.exists(self._state_path):
            logger.info("Attempting to load state from %s", self._state_path)
            with open(self._state_path) as fobj:
                state = pickle.load(fobj)
            self._tasks, self._workers = state
        else:
            logger.info("No prior state file exists at %s. Starting with clean slate", self._state_path)

    def dump(self):
        state = (self._tasks, self._workers)
        try:
            with open(self._state_path, 'w') as fobj:
                pickle.dump(state, fobj)
        except IOError:
            logger.warning("Failed saving scheduler state", exc_info=1)
        else:
            logger.info("Saved state in %s", self._state_path)

    def get_workers(self):
        """
        Return a dict of active workers.
        """
        return self._workers

    def set_worker(self, worker, timestamp):
        self._workers[worker] = timestamp

    def get_task(self, task_id):
        return self._tasks.get(task_id)

    def get_tasks(self):
        return self._tasks

    def find_or_add_task(self, task_id, task):
        return self._tasks.setdefault(task_id, task)

    def update_task(self, task_id, task):
        # noop: tasks are stored in RAM, so already saved
        return task

    def delete_tasks(self, task_ids):
        for task_id in task_ids:
            self._tasks.pop(task_id)

    def delete_workers(self, workers):
        # deactivate workers
        for worker in workers:
            self._workers.pop(worker)
