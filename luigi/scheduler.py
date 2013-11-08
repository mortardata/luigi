# Copyright (c) 2012 Spotify AB
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

import logging
import time
import task_history as history
from luigi.scheduler_engine.pickle_scheduler_engine import PickleSchedulerEngine
from luigi.scheduler_engine.scheduler_engine import Task as SchedulerTask

logger = logging.getLogger("luigi.server")

from task_status import PENDING, FAILED, DONE, RUNNING, UNKNOWN


class Scheduler(object):
    ''' Abstract base class

    Note that the methods all take string arguments, not Task objects...
    '''
    add_task = NotImplemented
    get_work = NotImplemented
    ping = NotImplemented

UPSTREAM_RUNNING = 'UPSTREAM_RUNNING'
UPSTREAM_MISSING_INPUT = 'UPSTREAM_MISSING_INPUT'
UPSTREAM_FAILED = 'UPSTREAM_FAILED'

UPSTREAM_SEVERITY_ORDER = ('', UPSTREAM_RUNNING, UPSTREAM_MISSING_INPUT, UPSTREAM_FAILED)
UPSTREAM_SEVERITY_KEY = lambda st: UPSTREAM_SEVERITY_ORDER.index(st)
STATUS_TO_UPSTREAM_MAP = {FAILED: UPSTREAM_FAILED, RUNNING: UPSTREAM_RUNNING, PENDING: UPSTREAM_MISSING_INPUT}

class Task(SchedulerTask):
    """
    TODO: Remove this once all existing pickle tasks have been
    migrated to new class location.
    """
    pass

class CentralPlannerScheduler(Scheduler):
    ''' Async scheduler that can handle multiple workers etc

    Can be run locally or on a server (using RemoteScheduler + server.Server).
    '''

    def __init__(self, retry_delay=900.0, remove_delay=600.0, worker_disconnect_delay=60.0, 
                 state_path='/var/lib/luigi-server/state.pickle', task_history=None, scheduler_engine=None):
        '''
        (all arguments are in seconds)
        Keyword Arguments:
        retry_delay -- How long after a Task fails to try it again, or -1 to never retry
        remove_delay -- How long after a Task finishes to remove it from the scheduler
        state_path -- Path to state file (tasks and active workers)
        worker_disconnect_delay -- If a worker hasn't communicated for this long, remove it from active workers
        '''
        self._retry_delay = retry_delay
        self._remove_delay = remove_delay
        self._worker_disconnect_delay = worker_disconnect_delay
        self._task_history = task_history or history.NopHistory()
        self._scheduler_engine = \
            scheduler_engine or PickleSchedulerEngine(state_path)

    def dump(self):
        self._scheduler_engine.dump()

    def load(self):
        self._scheduler_engine.load()

    def prune(self):
        """
        Prune the in-memory graph of workers and tasks, determining which ones are active and deleted.
        """
        logger.info("Starting pruning of task graph")
        
        # delete workers that haven't said anything for a while (probably killed)
        delete_workers = \
            [worker for worker, last_update_timestamp \
                     in self._scheduler_engine.get_workers().iteritems() \
                     if last_update_timestamp < (time.time() - self._worker_disconnect_delay)]
        if delete_workers:
            logger.info("workers %s timed out (no contact for >=%ss)", 
                        delete_workers, self._worker_disconnect_delay)
            self._scheduler_engine.delete_workers(delete_workers)

        # refetch workers to ensure that we have the latest list 
        workers = self._scheduler_engine.get_workers()
        remaining_workers = set(workers.keys())

        # Mark tasks with no remaining active stakeholders for deletion
        tasks = self._scheduler_engine.get_tasks()
        for task_id, task in tasks.iteritems():
            if not task.stakeholders.intersection(remaining_workers):
                if not task.remove:
                    logger.info("Task %r has stakeholders %r but none remain connected -> will remove task in %s seconds", task_id, task.stakeholders, self._remove_delay)
                    task.remove = time.time() + self._remove_delay
                    self._scheduler_engine.update_task(task_id, task)

            if task.status == RUNNING and task.worker_running and task.worker_running not in remaining_workers:
                # If a running worker disconnects, tag all its jobs as FAILED and subject it to the same retry logic
                logger.info("Task %r is marked as running by disconnected worker %r -> marking as FAILED with retry delay of %rs", task_id, task.worker_running, self._retry_delay)
                task.worker_running = None
                task.status = FAILED
                task.retry = time.time() + self._retry_delay
                self._scheduler_engine.update_task(task_id, task)

        # Prune remaining tasks
        remove_task_ids = []
        for task_id, task in tasks.iteritems():
            # remove tasks that have no stakeholders
            if task.remove and time.time() > task.remove:
                logger.info("Removing task %r (no connected stakeholders)", task_id)
                remove_task_ids.append(task_id)
            # Reset FAILED tasks to PENDING if max timeout is reached, and retry delay is >= 0
            elif task.status == FAILED and self._retry_delay >= 0 and task.retry < time.time():
                logger.info("Resetting failed task %r to PENDING to retry", task_id)
                task.status = PENDING
                self._scheduler_engine.update_task(task_id, task)

        if remove_task_ids:
            self._scheduler_engine.delete_tasks(remove_task_ids)
        logger.info("Done pruning task graph")

    def update(self, worker):
        # update timestamp so that we keep track
        # of whenever the worker was last active
        timestamp = time.time()
        self._scheduler_engine.set_worker(worker, timestamp)

    def add_task(self, worker, task_id, status=PENDING, runnable=True, deps=None, expl=None):
        """
        * Add task identified by task_id if it doesn't exist
        * If deps is not None, update dependency list
        * Update status of task
        * Add additional workers/stakeholders
        """
        self.update(worker)

        task = self._scheduler_engine.find_or_add_task(task_id, Task(PENDING, deps))

        if task.remove is not None:
            task.remove = None  # unmark task for removal so it isn't removed after being added

        if not (task.status == RUNNING and status == PENDING):
            # don't allow re-scheduling of task while it is running, it must either fail or succeed first
            task.status = status
            if status == FAILED:
                task.retry = time.time() + self._retry_delay

        if deps is not None:
            task.deps = set(deps)

        task.stakeholders.add(worker)

        if runnable:
            task.workers.add(worker)

        if expl is not None:
            task.expl = expl

        self._scheduler_engine.update_task(task_id, task)
        self._update_task_history(task_id, status)

    def get_work(self, worker, host=None):
        # TODO: remove any expired nodes

        # Algo: iterate over all nodes, find first node with no dependencies

        # TODO: remove tasks that can't be done, figure out if the worker has absolutely
        # nothing it can wait for

        # Return remaining tasks that have no FAILED descendents
        self.update(worker)
        best_t = float('inf')
        best_task = None
        locally_pending_tasks = 0
        running_tasks = []

        tasks = self._scheduler_engine.get_tasks()
        for task_id, task in tasks.iteritems():
            if worker not in task.workers:
                continue

            if task.status == RUNNING:
                running_tasks.append({'task_id': task_id, 'worker': task.worker_running})

            if task.status != PENDING:
                continue

            locally_pending_tasks += 1
            ok = True
            for dep in task.deps:
                if dep not in tasks:
                    ok = False
                elif tasks[dep].status != DONE:
                    ok = False

            if ok:
                if task.time < best_t:
                    best_t = task.time
                    best_task = task_id

        if best_task:
            t = tasks[best_task]
            t.status = RUNNING
            t.worker_running = worker
            self._scheduler_engine.update_task(best_task, t)
            self._update_task_history(best_task, RUNNING, host=host)

        return {'n_pending_tasks': locally_pending_tasks,
                'task_id': best_task,
                'running_tasks': running_tasks}

    def ping(self, worker):
        self.update(worker)

    def _upstream_status(self, task_id, upstream_status_table):
        if task_id in upstream_status_table:
            return upstream_status_table[task_id]

        tasks = self._scheduler_engine.get_tasks()
        if task_id in tasks:
            task_stack = [task_id]

            while task_stack:
                dep_id = task_stack.pop()
                if dep_id in tasks:
                    dep = tasks[dep_id]
                    if dep_id not in upstream_status_table:
                        if dep.status == PENDING and dep.deps:
                            task_stack = task_stack + [dep_id] + list(dep.deps)
                            upstream_status_table[dep_id] = ''  # will be updated postorder
                        else:
                            dep_status = STATUS_TO_UPSTREAM_MAP.get(dep.status, '')
                            upstream_status_table[dep_id] = dep_status
                    elif upstream_status_table[dep_id] == '' and dep.deps:
                        # This is the postorder update step when we set the
                        # status based on the previously calculated child elements
                        upstream_status = [upstream_status_table.get(id, '') for id in dep.deps]
                        upstream_status.append('')  # to handle empty list
                        status = max(upstream_status, key=UPSTREAM_SEVERITY_KEY)
                        upstream_status_table[dep_id] = status
            return upstream_status_table[dep_id]

    def _serialize_task(self, task_id):
        task = self._scheduler_engine.get_task(task_id)
        return {
            'deps': list(task.deps),
            'status': task.status,
            'workers': list(task.workers),
            'start_time': task.time,
            'params': self._get_task_params(task_id),
            'name': self._get_task_name(task_id)
        }

    def _get_task_params(self, task_id):
        params = {}
        params_part = task_id.split('(')[1].strip(')')
        params_strings = params_part.split(", ")

        for param in params_strings:
            if not param:
                continue
            split_param = param.split('=')
            if len(split_param) != 2:
                return {'<complex parameters>': params_part}
            params[split_param[0]] = split_param[1]
        return params

    def _get_task_name(self, task_id):
        return task_id.split('(')[0]

    def graph(self):
        self.prune()
        serialized = {}
        tasks = self._scheduler_engine.get_tasks()
        for task_id, task in tasks.iteritems():
            serialized[task_id] = self._serialize_task(task_id)
        return serialized

    def _recurse_deps(self, task_id, serialized, tasks=None):
        if not tasks:
            # fetch tasks once from the engine to avoid fetching
            # for each recursion
            tasks = self._scheduler_engine.get_tasks()

        if task_id not in serialized:
            task = tasks.get(task_id)
            if task is None:
                logger.warn('Missing task for id [%s]', task_id)
                serialized[task_id] = {
                    'deps': [],
                    'status': UNKNOWN,
                    'workers': [],
                    'start_time': UNKNOWN,
                    'params': self._get_task_params(task_id),
                    'name': self._get_task_name(task_id)
                }
            else:
                serialized[task_id] = self._serialize_task(task_id)
                for dep in task.deps:
                    self._recurse_deps(dep, serialized, tasks=tasks)

    def dep_graph(self, task_id):
        self.prune()
        serialized = {}
        task = self._scheduler_engine.get_task(task_id)
        if task:
            self._recurse_deps(task_id, serialized)
        return serialized

    def task_list(self, status, upstream_status):
        ''' query for a subset of tasks by status '''
        self.prune()
        result = {}
        upstream_status_table = {}  # used to memoize upstream status
        tasks = self._scheduler_engine.get_tasks()
        for task_id, task in tasks.iteritems():
            if not status or task.status == status:
                if (task.status != PENDING or not upstream_status or
                    upstream_status == self._upstream_status(task_id, upstream_status_table)):
                    serialized = self._serialize_task(task_id)
                    result[task_id] = serialized
        return result

    def fetch_error(self, task_id):
        task = self._scheduler_engine.get_task(task_id)
        if task.expl is not None:
            return {"taskId": task_id, "error": task.expl}
        else:
            return {"taskId": task_id, "error": ""}

    def _update_task_history(self, task_id, status, host=None):
        try:
            if status == DONE or status == FAILED:
                successful = (status == DONE)
                self._task_history.task_finished(task_id, successful)
            elif status == PENDING:
                self._task_history.task_scheduled(task_id)
            elif status == RUNNING:
                self._task_history.task_started(task_id, host)
        except:
            logger.warning("Error saving Task history", exc_info=1)

    @property
    def task_history(self):
        # Used by server.py to expose the calls
        return self._task_history
