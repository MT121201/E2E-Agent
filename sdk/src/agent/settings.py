from typing import Callable

from dotenv import find_dotenv
from pydantic import PositiveInt
from pydantic_settings import SettingsConfigDict
from redis.asyncio import Redis

from utilities.settings import Settings, Config
from utilities.settings.objest_storage import ObjectStorageSettings
from utilities.settings.redis_wrapper import RedisWrapperClient