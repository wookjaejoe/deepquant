from __future__ import annotations

from threading import Thread
import os
from typing import *

from django.apps import AppConfig

app_instances: Dict[str, DaemonAppConfig] = {}


class DaemonAppConfig(AppConfig):
    __instance = None

    def __init__(self, app_name, app_module):
        AppConfig.__init__(self, app_name, app_module)
        app_instances.update({app_name: self})

    def work(self):
        raise NotImplementedError()

    def ready(self):
        running_flag = f"{self.__class__.__name__.upper()}_IS_RUNNING"
        is_running = os.environ.get(running_flag)
        if is_running:
            return
        else:
            os.environ[running_flag] = "Running"

        thread = Thread(target=self.work, daemon=True)
        thread.start()
        self.__set_instance(self)

    @classmethod
    def __set_instance(cls, ins):
        cls.__instance = ins

    @classmethod
    def instance(cls):
        return cls.__instance
