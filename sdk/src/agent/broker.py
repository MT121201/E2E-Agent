import asyncio
import base64
import json
from abc import ABC
from contextlib import asynccontextmanager, AbstractAsyncContextManager, AsyncExitStack
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from inspect import iscoroutine
from typing import (
    Literal,
    Self,
    Awaitable,
    Any,
    Sequence,
    ClassVar,
    get_origin,
    get_args,
    AsyncIterator
)
from uuid import UUID

from cloudpickle import cloudpickle
from pydantic import Field, PrivateAttr, BaseModel, ConfigDict

from redis import WatchError
from redis.asyncio import Redis
from redis.asyncio.client import Pipeline
from redis.exceptions import LockError
from redis.typing import FieldT, EncodableT


from .settings import REDIS, CONFIG, get_redis
from utilities.func import encrypt, decrypt, dump_if_base_model
from utilities.logger import logger