import logging

from django.apps import AppConfig
from core.quantpick import QuantPicker

_logger = logging.getLogger(__name__)


class DeepQuantConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'quant'

    def __init__(self, app_name, app_module):
        AppConfig.__init__(self, app_name, app_module)

    def ready(self):
        # singleton 인스턴스를 호출하여 미리 초기화
        QuantPicker.instance()
