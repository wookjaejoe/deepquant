from slowapi import Limiter
from slowapi.util import get_remote_address

limit_by_address = Limiter(key_func=get_remote_address).limit
