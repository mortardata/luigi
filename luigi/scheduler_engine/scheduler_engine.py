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
    def get_task(self):
        pass

    @abc.abstractmethod
    def get_tasks(self):
        pass

    @abc.abstractmethod
    def add_task(self, task_id, task):
        pass

    @abc.abstractmethod
    def update_task(self, task_id, task):
        pass

    @abc.abstractmethod
    def delete_tasks(self, task_ids):
        pass
