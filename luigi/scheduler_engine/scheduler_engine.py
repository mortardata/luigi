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

import abc
import time

class Task(object):
    def __init__(self, status, deps, stakeholders=None, workers=None, timestamp=None, 
        retry=None, remove=None, worker_running=None, expl=None):
        """"""
        self.stakeholders = set(stakeholders) if stakeholders else set() # workers that are somehow related to this task (i.e. don't prune while any of these workers are still active)
        self.workers = set(workers) if workers else set() # workers that can perform task - task is 'BROKEN' if none of these workers are active
        self.deps = set(deps) if deps else set()
        self.status = status  # PENDING, RUNNING, FAILED or DONE
        self.time = timestamp or time.time()
        self.retry = retry
        self.remove = remove
        self.worker_running = worker_running  # the worker that is currently running the task or None
        self.expl = expl

    def __repr__(self):
        return "Task(%r)" % vars(self)

class SchedulerEngine(object):
    """
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def load(self):
        pass

    @abc.abstractmethod
    def dump(self):
        pass

    @abc.abstractmethod
    def get_workers(self):
        pass

    @abc.abstractmethod
    def set_worker(self, worker, timestamp):
        pass

    @abc.abstractmethod
    def delete_workers(self, workers):
        pass

    @abc.abstractmethod
    def get_task(self, task_id):
        pass

    @abc.abstractmethod
    def get_tasks(self):
        pass

    @abc.abstractmethod
    def find_or_add_task(self, task_id, task):
        pass

    @abc.abstractmethod
    def update_task(self, task_id, task):
        pass

    @abc.abstractmethod
    def delete_tasks(self, task_ids):
        pass
