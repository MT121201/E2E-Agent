
import time

from chatbone_apps.commons import (
    CHATBONE_ASSISTANT_APP_POSTFIX,
    CHATBONE_ASSISTANT_APP_PREFIX,
)

start = time.time()

from contextlib import contextmanager
from contextvars import ContextVar
import dataclasses
from datetime import timedelta, datetime
from enum import Enum
import json
from pprint import pformat
from time import time
from types import NoneType, UnionType
from uuid import UUID
from uuid_extensions import uuid7
from pydantic.fields import FieldInfo, PrivateAttr, _Unset
from pydantic import (
    BaseModel, 
    ConfigDict,
    Field,
    NonNegativeInt,
    TypeAdapter,
    create_model,
    model_validator, 
)
from typing import (
    AsyncGenerator,
    Type,
    ClassVar,
    Callable,
    Self,
    Any,
    get_args,
    Sequence,
    Literal,
    Annotated,
    get_origin,
    Generator,
    Awaitable,
)
import filetype
from filetype import match
from filetype.types.image import Jpeg
from filetype.types import document, IMAGE, VIDEO, AUDIO, archive
import asyncio
from ray import serve
from ray.exceptions import TaskCancelledError
from ray.serve._private.common import ReplicaState
from ray.serve.exceptions import RequestCancelledError
from ray.serve.handle import DeploymentHandle
import ray.serve.schema as ray_schema

from pydantic_core import PydanticUndefined

from utilities.func import utc_now
from utilities.logger import logger
from utilities.misc import UniversalLock

from .settings import OBJ_STORAGE, CONFIG
from .broker import (
    StreamData,
    StreamPair,
    DisplayMessage,
    DataSegment,
    UserData,
)


class ONLY(str, Enum):
    INPUT = "input"
    OUTPUT = "output"
    NONE = "none"


class BaseAssistantType(BaseModel):
    """
    Base class for assistant-related data types.

    This model introduces two key mechanisms that control how data classes
    derived from it are used in the user ↔ assistant communication pipeline:

    Attributes
    ----------
    only : ClassVar[Only]
        Directional restriction flag.
        - Only.OUTPUT → Class can only be produced by the assistant and sent
          to the user. Users are not allowed to provide it.
        - Only.INPUT → Class can only be provided by the user. Assistant must
          not send it back.
        - Only.NONE → No restrictions; can be used in both directions.

    to_user : bool
        Visibility flag for assistant → client/server (AS2CS) communication.
        - True → Data is intended to be shown to the end user.
        - False → Data is only for storage/logging and must not be displayed
          to the user.
        This flag is ignored for client/server → assistant (CS2AS) data flow.

    Notes
    -----
    - Subclasses should override `only` when they need to restrict the
      allowed communication direction.
    - `to_user` should be set per-instance depending on whether the data
      is user-facing or internal.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    only: ClassVar[ONLY] = ONLY.NONE
    to_user: bool = True


class MediaType(str, Enum):
    TEXT = "text"
    IMAGE = "image"
    AUDIO = "audio"
    VIDEO = "video"
    FILE = "file"


class InvalidFileTypeException(Exception):
    pass


class InvalidFileExtension(InvalidFileTypeException):
    def __init__(self, m="", allowed_ex=None, received_ex=None, filename=None):
        self.al_ex = allowed_ex
        self.re_ex = received_ex
        self.filename = filename
        if self.al_ex:
            m += f"Allowed extension: {allowed_ex}. "
        if self.re_ex:
            m += f"Got {received_ex}. "
        super().__init__(m)


class InvalidBinaryFile(InvalidFileTypeException):
    pass


class MediaObject(BaseAssistantType):
    """
    Represents a media file used as input to the assistant.

    A MediaObject holds the metadata required to validate, classify, and
    store a file. Subclasses typically define their own `type`, `mimes`,
    `extensions`, and `matchers` to restrict what kinds of files they
    represent (e.g., ImageMediaObject, AudioMediaObject).

    Notes
    -----
    - Assistant input is the collection of these objects.
      Users must provide all required MediaObject instances to call
      the assistant.
    - The `get_upload_url` helper (not shown here) is typically used
      to obtain a presigned URL for uploading the actual file content.

    Class Variables
    ---------------
    object_storage : ObjectStorageSettings
        Backend configuration for storing/retrieving the object.
    type : MediaType
        High-level classification of the media (e.g., image, video).
    matchers : Sequence[filetype.Type]
        File signature matchers used to detect the file type.
    mimes : list[str]
        Allowed MIME types for this media class.
    extensions : list[str]
        Allowed file extensions for this media class.
    """

    object_storage: ClassVar[ObjectStorageSettings] = OBJ_STORAGE #NOTE: define OBJ_STORAGE in settings.py
    type: ClassVar[MediaType] = None
    matchers: ClassVar[Sequence[filetype.type]] = None
    mimes: ClassVar[list[str]] = None
    extensions: ClassVar[list[str]] = None

    model_config = ConfigDict(frozen=True)
    object_name: str
    mime: str

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs) -> None:
        assert cls.type in MediaType
        cls.mimes = []
        cls.extensions = []
        for m in cls.matchers:
            cls.mimes.append(m.mime)
            cls.extensions.append(m.extension)

    @classmethod
    async def get_upload_url(
        cls,
        object_name: str,
        extension: str = None,
        expires: timedelta = timedelta(days=7),
        *,
        tagging: dict[str, str] | None = None,
    ) -> str:
        """
        This method will infer content-type using filename extension (if extension not provided) and the class.type .

        Args:
                object_name:
                expires:
                extension:
                tagging:
        Raises:
                InvalidFileExtension: When cannot infer extension or extension is not supported.
        Returns:
                Upload url. Call 'PUT' to push object to storage server
        """

        def _validate_extension() -> str:
            if extension is None:
                if "." in object_name:
                    ex = object_name.rsplit(".", 1)[-1]
                else:
                    raise InvalidFileExtension(
                        f"Cannot infer extension from object name. There is no separator '.' "
                    )
            else:
                assert isinstance(extension, str)
                ex = extension
            for i, ext in enumerate(cls.extensions):
                if ex == ext:
                    return cls.mimes[i]
            raise InvalidFileExtension(
                "Extension is not supported. ", cls.extensions, ex
            )

        mime = await asyncio.to_thread(_validate_extension)
        response_headers = {"response-content-type": "image/png"} #NOTE: Hardcode to png for now
        return await OBJ_STORAGE.get_upload_url(
            object_name,
            expires,
            response_headers=response_headers,
            content_type=mime,
            tagging=tagging,
        )
    
    @classmethod
    async def validate_object(
        cls,
        object_name: str,
        rmove_if_invalid: bool = True,
    ) -> Self:
        """
        Validate the object store in server. This method should be called after object put to server
        Args:
            Object_name
            rmove_if_invalid: If True, remove the object if it is invalid
            
        Raises:
            InvalidBinaryFile: If the object type is invalid
        Returns:
            MediaObject instance
        """
        magic = await OBJ_STORAGE.get_object(object_name, lenght=8192)
        try:
            mime = await cls.get_mime(magic)
            return cls(object_name=object_name, mime=mime)
        except InvalidBinaryFile as e:
            if rmove_if_invalid:
                await OBJ_STORAGE.remove_object(object_name)
            raise 

    @classmethod
    async def put_object(
        cls,
        object_name: str,
        data,
    ) -> Self:
        """
        Put the object to server and validate it
        Args:
            Object_name
            data: binary data
        Raises:
                InvalidBinaryFile: If the object is not supported type.
        Returns: MediaObject instance
        """
        mime = await cls.get_mime(data)
        await OBJ_STORAGE.put_object(object_name, data, mime)
        return cls(object_name=object_name, mime=mime)
    
    @classmethod
    async def get_mime(cls, data) -> str:
        """
        Get mime of the "bytes" data.
        Args:
                data:
        Raises:
                InvalidBinaryFile: If data is not supported.
        Returns:
                mime string.
        """
        if (m:= asyncio.to_thread(match, data, cls.matchers)) is not None:
            return m.mime
        raise InvalidBinaryFile("File type is not supported.")

    async def get_preview_url(self, expires: timedelta = timedelta(days=7)):
        return await OBJ_STORAGE.get_download_url(
            self.object_name,
            expires,
            response_headers={  # DEFAULT disposition is inline.
                "response-content-disposition": "inline",
                "response-content-type": self.mime,
            },
        )

    async def get_object(self) -> bytes:
        """Get the binary object from server."""
        return await OBJ_STORAGE.get_object(self.object_name)

    async def remove_object(self):
        """Remove the binary object from server"""
        return await OBJ_STORAGE.remove_object(self.object_name)
    


# noinspection PyTypeChecker
AssistantDataType_T: tuple[Type[BaseAssistantType]] = ()
"""Assistant datatype in tuple format, use this to test with isinstance()."""

AssistantDataType_U: Type[BaseAssistantType] = BaseAssistantType
"""Assistant datatype in union format."""


AnyMediaObject: Type[MediaObject] = MediaObject
"""Media object union."""


def assistant_datatype(cls_type):
    """Decorator to assign assistant datatype.
    This type is the one stream to app, so it should be pickleable and clear purpose to show to user.
    Or to be user input type.
    """
    global AssistantDataType_T, AssistantDataType_U, AnyMediaObject
    assert issubclass(cls_type, BaseAssistantType) and cls_type is not BaseAssistantType

    if AssistantDataType_U == BaseAssistantType:
        AssistantDataType_U = cls_type
    else:
        AssistantDataType_U = AssistantDataType_U | cls_type

    AssistantDataType_T = AssistantDataType_T + (cls_type,)

    if issubclass(cls_type, MediaObject):
        if AnyMediaObject is MediaObject:
            AnyMediaObject = cls_type
        else:
            AnyMediaObject = AnyMediaObject | cls_type

    return cls_type

@assistant_datatype
class ImageObject(MediaObject):
    class _Jpeg(filetype.Type):
        EXTENSION = "jpg"
        MIME = "image/jpeg"
        
        def __init__(self):
            self._jped = Jpeg()
            super().__init__(self.MIME, self.EXTENSION)

        def match(self, buf):
            return self._jped.match(buf)
        
    type = MediaType.IMAGE
    matchers= IMAGE + _Jpeg()


@assistant_datatype
class VideoObject(MediaObject):
    type = MediaType.VIDEO
    matchers = VIDEO


@assistant_datatype
class AudioObject(MediaObject):
    type = MediaType.AUDIO
    matchers = AUDIO


@assistant_datatype
class DocumentObject(MediaObject):
    type = MediaType.DOCUMENT
    matchers = (
        document.Doc(),
        document.Docx(),
        document.Ppt(),
        archive.Pdf(),
        archive.Epub(),
    )  # TODO: support txt


@dataclasses.dataclass
class InputFilter:
    regex_string: str
    allow: bool = True
    replacement_string: str = ""
    multiline: bool = False
    case_sensitive: bool = True
    unicode: bool = False
    dot_all: bool = False


@assistant_datatype
class Text(BaseAssistantType):
    input_filter: ClassVar[InputFilter | None] = None
    """Filter the input of user. This is prevent user typing"""
    input_validator: ClassVar[[Callable[[str], str | None | Awaitable[str | None]]]] = (
        lambda x: x
    )
    content: str

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) ->None:
        if cls.input_filter:
            assert isinstance(cls.input_filter, InputFilter)

    
@assistant_datatype
class Selection(BaseAssistantType):
    model_config = ConfigDict(from_attributes=True)
    __adapter: ClassVar[TypeAdapter] = TypeAdapter(dict[str, str | None])

    only = ONLY.INPUT
    options: ClassVar[dict[str, str | None]] = None
    """Options for user to select from. Key is the value, value is the description."""
    selection: str

    def __class_getitem__(cls, items: dict[str, str | None]) -> Type[Self]:
        return cls.create_from_options(**items)
    
    @classmethod
    def create_from_options(cls, **kwargs) -> Type[Self]:
        """
        Args:
            options:keys are the option key, which one that user will choose, values are description, give user a hint.
            __doc__:

        Examples:
            CustomSelection = Selection.create_from_options(name = "give me a name", value = "what is your age")
            print(CustomSelection)
            assert issubclass(CustomSelection, Selection)

            c = CustomSelection.model_validate({"selection":"name"})
            try:
                d = CustomSelection(selection="abc")
            except ValueError as e:
                print(e)
            print(c)
            assert isinstance(c,Selection)

        Returns:
            New subclass of Selection.
        """
        doc = kwargs.pop("__doc__", None)
        module = kwargs.pop("__module__", cls.__module__)
        sorted_keys = sorted(kwargs.keys())
        model_name = f"{cls.__name__}_options_{'_'.join(sorted_keys)}"

        model = create_model(
            model_name,
            __doc__=doc,
            __base__=cls,
            __module__=module,
            options=(ClassVar[dict[str, str | None]], kwargs),
            _dynamic_construction_class_args__=(
                ClassVar[tuple],
                (kwargs, __doc__),
            ),
        )

        logger.debug(f"Dynamically create {model.__name__}, {model.__module__}")
        return model

    @model_validator(mode="after")
    def check_selected(self) -> Self:
        logger.debug(f"Validating selection: {self.options}")
        _ = self.__adapter.validate_python(self.options)
        if (var:= self.selection) not in self.options:
            raise ValueError(f"Selection {var} is not in options {list(self.options.keys())}")
        return self
    

def selection_reconstruct(args, values):
    model = Selection.create_from_options(**args)
    logger.debug(f"Reconstruct selection model: {model}, {model.__module__}")
    assert issubclass(model, Selection)
    return model(**values)


assistant_datatype_strings = [t.__name__ for t in AssistantDataType_T]
assistant_datatype_dict = {t.__name__: t for t in AssistantDataType_T}
logger.debug(
    f"Assistant datatype dict:\n"
    f"{pformat({k:str(v) for k,v in assistant_datatype_dict.items()}, indent=4)}"
)
   

class DataForm(BaseModel):
    annotation: Any

    # pydatic field parameters
    default: Any = PydanticUndefined
    description: Any = _Unset

    # For Selection
    options: dict[str, str | None]


class CreateForm(BaseModel):
    data: dict[str, DataForm]
    """Dict with keys as attribute names and values as AssistantDataType Field parameters. """

    doc: str| None = None
    model_name: str | None = None


# noinspection PyTypeChecker
class AssistantData(StreamData, DisplayMessage):
    model_config = ConfigDict(validate_default=True, validate_assignment=True)
    _is_input_schema: ClassVar[bool] = False
    """True if this schema is for input to assistant, False if this schema is for output
    """
    _default_exclude_attrs: ClassVar[list[str]] = list(
        DisplayMessage.model_fields.keys()
    )
    """Attributes from DisplayMessage"""
    _exclude_attrs: ClassVar[list[str]] = None

    # T: Tuple ; U: Union
    T: ClassVar[Any] = AssistantDataType_T
    U: ClassVar[Any] = AssistantDataType_U

    _chat_context_id: UUID = PrivateAttr(default_factory=uuid7)
    """All data including input, status code, request input and output data in one chat unique uuid.
    This is set by app, not user or dev.
    """
    _created_at: datetime = PrivateAttr(default_factory=utc_now)
    @property
    def chat_context_id(self):
        assert self._chat_context_id is not None
        return self._chat_context_id
    
    @property
    def created_at(self):
        assert self._created_at is not None
        return self._created_at
    
    @classmethod
    def iter_data_fields(cls) -> Generator[tuple[str, FieldInfo], None, None]:
        for name, field_info in cls.model_fields.items():
            if (
                get_origin(field_info.annotation) == ClassVar
                or name in cls.__exclude_attrs
            ):
                continue
            yield name, field_info

    @classmethod
    def __pydantic_init_subclass__(cls):
        cls._exclude_attrs = cls._exclude_attrs or []
        if cls._default_exclude_attrs is not None:
            cls._exclude_attrs.extend(cls._default_exclude_attrs)
        fields: dict[str, FieldInfo] = cls.model_fields
        for name, field_info in fields.items():
            ann = field_info.annotation
            if (
                get_origin(field_info.annotation) == ClassVar
                or name in cls._exclude_attrs
            ):
                continue
            if field_info.metadata:
                ann = Annotated[ann, *field_info.metadata]
            cls._validate_schema(name, ann)
        
    @classmethod
    def _validate_schema(cls, name: str, ann: Type[Any]):
        # General supported type examples: ImageObject, list[ImageObject]|VideoObject, ImageObject|VideoObject|None
        # validate recursively through list.
        org = get_origin(ann)
        if org in (
            None,
            Annotated,
        ):  # Annotated is only supported for the annotation type in cls.T
            if not ann in cls.T and not issubclass(ann, cls.T):
                m = f"Does not support field annotation '{ann.__name__}' of '{cls.__name__}.{name}'.Type hint must be in {[t.__name__ for t in cls.T]}."
                raise ValueError(m)
            if cls._is_input_schema:
                assert issubclass(ann, BaseAssistantType)
                if ann.only == ONLY.OUTPUT:
                    m = f"Input schema can not contain output-only type '{ann.__name__}' of {cls.__name__}.{name}'."
                    raise ValueError(m)
        else:
            if issubclass(org, (list, UnionType)):
                new_anns = list(get_args(ann))
                if NoneType in new_anns:
                    assert len(new_anns) > 1
                    new_anns.remove(NoneType)
                for ann in new_anns:
                    cls._validate_schema(name, ann)

                

    @classmethod
    def _get_model_name(cls):
        return uuid7(as_type="str").replace("-", cls.__name__)

    @classmethod
    def _get_module_name(cls):
        return cls.__module__

    @classmethod
    def _validate_model(cls, schema: dict[str, Any]):
        for name, field in schema.items():
            # If not tuple, create simple field for compatible with the latest pydantic versions. While old version not support non-tuple.
            if not isinstance(field, Sequence):
                schema[name] = (field, Field())
            ann = schema[name][0]  # extract annotation, ignore default value
            cls._validate_schema(name, ann)

    def _parse_assistant_datatype_ann(self, typehint: str) -> type[Any]:
        """from "Text|Video" ... ->"""
        pass

    @classmethod
    def create_model(
        cls, model_schema: dict[str, Any], doc: str | None = None
    ) -> Type["AssistantData"]:
        """
        todo: is this method necessary? or just directly inherit ? NO IT IS USED TO DYNAMICALLY GEN BY LLM.
        Create a data model dynamically.
        Args:
                model_schema: dictionary with keys as name and
                doc
        Returns:
                Instance of a subclass of AssistantData
        """
        cls._validate_model(model_schema)
        return create_model(
            cls._get_model_name(),
            __base__=cls,
            __module__=cls._get_module_name(),
            __doc__=doc,
            **model_schema,
        )

    @classmethod
    def from_json_schema(cls, schema: str):
        """
        Request some input object from the user with specified object name, object type, and description.
            Args:
                schema: a dictionary with keys as a string type represent the short name of the request object, for example "math_document", "cat_picture".
                     Values of a dict, according to the keys, is tuple of length 3, both are strings.
                      - The first value of the tuple is type of requested object, it must be in this list: {assistant_datatype_strings}.
                      - The second is the text represent description or hint to show to user. If description is not given, it must be the blank string "".
                      - The third must be "optional" or "require", user has only two options, give all the object marked as "require" or refuse to give any information.

                    **field_definitions: Field definitions of the new model. Either:

                    - a single element, representing the type annotation of the field.
                    - a two-tuple, the first element being the type and the second element the assigned value
                      (either a default or the [`Field()`][pydantic.Field] function).

        Returns:
                AssistantData object containing all user inputs, or None if user refuse to give.
        """
        d = json.loads(schema)


class AssistantInputData(AssistantData):
    _exclude_attrs = ["context", "user_name"]
    _is_input_schema = True

    _user_name: str
    _context: Any = None
    """Get from Context collector """

    @property
    def user_name(self) -> str:
        return self._user_name
    
    @property
    def context(self):
        return self._context


class AssistantStatusCode(str, Enum):
    START = "start"
    DONE = "done"
    SUCCESS = "success"
    ERROR = "error"
    CANCELED = "canceled"
    CANCELING = "canceling"
    PROCESSING = "processing"


class Status(BaseModel):
    code: AssistantStatusCode
    detail: str | None = None


class AssistantOutputData(AssistantData):
    _exclude_attrs = ["stream_place"]
    stream_place: int =0
    """The stream drain place in display message"""
    
    # This set by app, not user or dev.
    _assistant_name: str | None = PrivateAttr(default=None)
    _status: Status | None = PrivateAttr(None)
    _chuck_order: NonNegativeInt | None = PrivateAttr(None)

    @property
    def status(self):
        assert self._status is not None
        return self._status
    
    @property
    def assistant_name(self):
        return self._assistant_name
    
    @property
    def chuck_order(self):
        assert self._chuck_order is not None
        return self._chuck_order


class AssistantStreamContext(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    stream_pair: StreamPair
    chat_context_id: UUID
    cs_id: UUID
    userdata: UserData

    _lock: UniversalLock = PrivateAttr(default_factory=UniversalLock)


_assistant_stream_context: ContextVar[AssistantStreamContext] = ContextVar(
"assistant_stream_context"
)


@contextmanager
def _stream_cm(context: AssistantStreamContext):
    token = _assistant_stream_context.set(context)
    yield token
    _assistant_stream_context.reset(token)


class RequestedInput(StreamData):
    model_config = ConfigDict(arbitrary_types_allowed=True, validate_assignment=True)
    request_schema: Type[AssistantInputData]
    id: UUID = Field(default_factory=uuid7, frozen=True)
    """The receiver will check for this id to know if it is a response for the request"""

    _data: AssistantInputData | None = None
    _is_response: bool = False

    def is_response_of(self, uid: UUID|None = None) -> bool:
        if uid:
            return self._is_response and self.id == uid 
        return self._is_response
    
    @property
    def data(self):
        assert self.is_response and isinstance(self._data, AssistantInputData)
        return self._data

    def set_response_data(self, response_data: Any) -> None:
        self._data =  self.request_schema.model_validate(response_data)
        self._is_response = True

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        assert issubclass(cls.request_schema, AssistantInputData)


async def create_input_data_model():
    pass


async def create_output_data_model():
    pass


async def create_input_request():
    pass


async def send_input_request(request: RequestedInput):
    pass


def format_doc(func):
    func.__doc__ = func.__doc__.format(
        assistant_datatype_strings=assistant_datatype_strings
    )
    return func


# noinspection PyUnresolvedReferences,PyIncorrectDocstring
@format_doc
async def request_user_input(
    request_schema: dict[str, tuple[str, str, Literal["optional", "required"]]],
    prompt: str,
    timeout: int | None = None,
) -> AssistantData | None: 
    """
    Request some input object from the user with specified object name, object type, and description.
    Args:
            request_schema: a dictionary with keys as a string type represent the short name of the request object, for example "math_document", "cat_picture".
             Values of a dict, according to the keys, is tuple of length 3, both are strings.
              - The first value of the tuple is type of requested object, it must be in this list: {assistant_datatype_strings}.
              - The second is the text represent description or hint to show to user. If description is not given, it must be the blank string "".
              - The third must be "optional" or "require", user has only two options, give all the object marked as "require" or refuse to give any information.

    Returns:
            AssistantData object containing all user inputs, or None if user refuse to give.
    """
    # noinspection PyTypeChecker
    def _make_field():
        for k, v in request_schema.items():
            t, desc, opt = v
            field: FieldInfo = Field()
            if (dtype := globals().get(t)) is None:
                raise ValueError(
                    f"Type '{t}' of request schema '{k}' is not supported. Supported types are {assistant_datatype_strings}"
                )
            if opt == "optional":
                dtype = dtype | NoneType
                field.default = None
            elif opt != "required":
                raise ValueError(
                    f"Option '{opt}' of request schema '{k}' is not supported. Supported options are 'optional' and 'required'."
                )
            field.description = desc
            request_schema[k] = (dtype, field)
        
    schema = await asyncio.to_thread(_make_field)
    requested_input: RequestedInput = asyncio.to_thread(
        AssistantData._create_request, schema
    )

    write_stream, read_stream = _assistant_stream_context.get()
    logger.info(
         f"Sending input request to user through stream with key '{write_stream.key}'..."
    )
    stream_id = await write_stream.write(requested_input)
    logger.info(f"Sent input request to user through stream id '{stream_id}'")

    # wait for get valid requested input object, or until setting.config.request_user_input_timeout
    # noinspection PyUnresolvedReferences
    async def _wait_task():
        try:
            async for data in read_stream.bind("$"):
                for d in data:
                    if isinstance(d, RequestedInput):
                        if data.id == requested_input.id:
                            # NOTE: return only the first reach valid data came after the time of waiting, it's enough I thought. =))
                            return data.data
                        logger.warning(
                            f"Got RequestInput object but not match with the id of the object sent. Ignore and continue waiting."
                        )
        except asyncio.CancelledError:
            logger.debug("The task of waiting for requested user input is cancelled.")
            raise
    
    try:
        logger.info(
            f"Waiting for stream with key '{read_stream.key}' for requested input object."
        )
        return await asyncio.wait_for(_wait_task(), CONFIG.request_user_input_timeout)
    except asyncio.TimeoutError:
        logger.warning(
            f"Timeout waiting for requested input object after {CONFIG.request_user_input_timeout} seconds."
        )
        return None
    
async def capture_chat_context(
    from_latest: bool = True,
    count: int | None = None,
    drop_if_input_only: bool = True,
    data_only: bool = True,
) -> list[list[AssistantData]]:
    """Retrieve latest data,
    Args:
        from_latest: The latest group of data which has the same chat_context_id will be in the first list index.
        count: The number of data retrieved from stream ( Not the data after filter by status code.)
        drop_if_input_only: Whether to drop the context if there is only AssistantInput init. It's useful to drop
         in the case that you just want to collect the complete contexts, which including both input and output.
        data_only: If false, return both data input and status code.

    Returns: a list as matrix with row is list of all data that has the same chat_context_id.

    IMPORTANT:
        All data that have the same chat_context_id must be next to each others.
    """
    read_stream = _assistant_stream_context.get().stream_pair.read_stream
    all_data = await read_stream.capture(from_latest, count)
    logger.debug(f"All data captured (len={len(all_data)}): {all_data}"[:100])

    chat_context_list: list[list[AssistantData]] = []
    context_dict: dict[UUID, list[AssistantData] | None]= {}

    def _do():
        current_id: UUID | None = None
        for data in all_data:
            assert isinstance(data, AssistantData)

            # Not have data yet
            if (
                isinstance(data, AssistantOutputData) and
                data.status.code != AssistantStatusCode.PROCESSING and
                data_only
            ):
                logger.debug(
                    f"Captured AssistantOutputData but status is not PROCESSING, and be skipped because of data_only:\n"
                    f"{data}"
                )
                continue
            
            # New ID notice
            if current_id != data.chat_context_id:
                assert data.chat_context_id is not None

                # Handle last context 
                if current_id is not None:
                    if from_latest:
                        context_dict[current_id].reverse()
                    chat_context_list.append(context_dict[current_id])
                    # None mark  it done, future will raise error if same id
                    context_dict[current_id] = None

                # Switch next id
                context_dict[data.chat_context_id]= [data]
                current_id = data.chat_context_id

            # Same ID
            else:
                context_dict[current_id].append(data)

        # Save the last context, which is not triggered because there is no more satisfy data.chat_context_id != current_id
        if current_id:
            if from_latest:
                context_dict[current_id].reverse()
            chat_context_list.append(context_dict[current_id])
            logger.debug("Saved last context")

        if drop_if_input_only:
            for context in chat_context_list:
                if len(context) == 1:
                    if isinstance(context[0], AssistantInputData):
                        chat_context_list.remove(context)

                    else:
                        logger.error(
                             f"Unbehavior, capture only one chunk but it's not the AssistantInputData instance."
                        )

    await asyncio.to_thread(_do)
    logger.debug(f"returned chat_context_list len {len(chat_context_list)}")
    return chat_context_list


async def get_data_segments(n_latest: int = -1) -> list[DataSegment]:
    """
    Get n latest data segments.
    Args:
        n_latest: If <=0, get all segments.

    Returns:
        list of DataSegment in the earliest to latest order.
    """
    context = _assistant_stream_context.get()
    cs = (await context.userdata.get_chat_sessions([context.cs_id]))[context.cs_id]
    data_segments = []
    if n_latest <= 0:
        filter_ds = cs.data_segments
    else:
        filter_ds = cs.data_segments[-n_latest:]
    
    for encoded in filter_ds:
        ds = await asyncio.to_thread(DataSegment.decode, encoded)
        assert isinstance(ds, DataSegment)
        data_segments.append(ds)
    
    return data_segments


async def save_data_segments(data_segments: list[DataSegment], *, persist: bool = True):
    context = _assistant_stream_context.get()
    encoded_segs: list[str] = []
    async with context._lock:
        cs = (await context.userdata.get_chat_sessions([context.cs_id]))[context.cs_id]
        for encoded in data_segments:
            ds = await asyncio.to_thread(ds.encode)
            encoded_segs.append(ds)

    if persist:
            logger.warning("Persist not implementation.")
            # TODO


async def get_user_facts() -> list[str]:
    context = _assistant_stream_context.get()
    return context.userdata.summaries


async def save_user_facts(facts: list[str], *, persist: bool = True):
    context = _assistant_stream_context.get()
    async with context._lock:
        await context.userdata.append("summaries", facts)
    if persist:
        logger.warning("Persis not implementation.")
        # todo



AssistantStreamer = Callable[
    [AssistantInputData], AsyncGenerator[AssistantOutputData, None]
]


class BaseAssistant(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, validate_default=True, frozen=True)
    streamer: AssistantStreamer
    input_schema: Type[AssistantInputData]
    name: str
    
    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs):
        fields = cls.model_fields
        if (st:= fields["streamer"].annotations) != AssistantStreamer:
            m = f"Typehint of 'streamer' must be 'AssistantStreamer'. Got '{st}'."
            raise(m)
        
        if (it := fields["input_schema"].annotation) not in (
            type[AssistantInputData],
            Type[AssistantInputData],
        ):
            m = f"Typehint of 'input_schema' must be 'Type[AssistantInputData]'. Got '{it}'."
            raise TypeError(m)
        
        assert fields["name"].annotation == str

    async def handle_cancellation(self):
        pass


class UserInputData(BaseModel):
    assistant_name: str | None = None
    data: AssistantInputData | None = None


class AssistantInterface:
    """Chat app uses this class to communicate with assistant app."""
    @staticmethod
    async def get_assistant_names() -> dict[str, str]:
        """

        Returns:
            A dict with format (assistant_name, app_name) 'app_name' are serve app_name, for internal use to get DeploymentHandle,
            'assistant_name' is the one show to app or user.
        """
        names = {}
        app_names = await asyncio.to_thread(
            AssistantInterface._get_healthy_assistant_app_names
        )
        for appname in app_names:
            handle = await AssistantInterface.get_assistant_app_handle(appname)
            as_name = await handle.get_name.remote()
            names[as_name] = appname
        return names
    
    @staticmethod
    async def get_assistant_schema(assistant_app_name: str) -> Type[AssistantInputData]:
        handle: DeploymentHandle = await AssistantInterface.get_assistant_app_handle(
            assistant_app_name
        )
        schema = await handle.get_schema.remote()
        assert issubclass(schema, AssistantInputData)
        return schema
    
    @staticmethod
    async def get_assistant_app_handle(assistant_app_name: str) -> DeploymentHandle:
        return await asyncio.to_thread(
            AssistantInterface._get_assistant_app_handle, assistant_app_name
        )

    @staticmethod
    async def call( 
        app_name,   
        user_input: UserInputData,  
        stream_pair: StreamPair,    
        userdata: UserData, 
        cs_id: UUID,    
    ):  
        """Call ray task and block until success""" 
        handle = await AssistantInterface.get_assistant_app_handle(app_name)
        task = handle.remote(user_input.data, stream_pair, userdata, cs_id)

        logger.debug(
            f"AssistantInterface called assistant {user_input.assistant_name}. chat_context_id = {user_input.data.chat_context_id}"
        )

        try:
            await task
        except asyncio.CancelledError as e:
            logger.debug(f"Assistant call cancelling...")
            task.cancel()
            try:
                await task
            except RequestCancelledError as e:
                if not isinstance(e, TaskCancelledError):
                    logger.exception(e)
                    raise e
                logger.debug(f"Ray cancelled successfully")
        except Exception as e:
            logger.exception(e)
            raise e
        
        @staticmethod
        def _get_assistant_app_handle(assistant_name: str) -> DeploymentHandle:
            return serve.get_app_handle(assistant_name)
        
        @staticmethod
        def _get_healthy_assistant_app_names() -> list[str]:
            assistants= []
            apps = serve.status().applications
            print(f"serve.status().applications return {apps}")
            for name, status in apps.items():
                if status.status == ray_schema.ApplicationStatus.RUNNING:
                    # Check healthy for all deployments and detect one name has assistant postfix
                    has_assistant_postfix: bool =False
                    has_one_deployment_not_healthy: bool = False
                    for depl_name, depl_status in status.deployments.items():
                        if depl_name.endswith(
                        CHATBONE_ASSISTANT_APP_POSTFIX
                        ) and depl_name.startswith(CHATBONE_ASSISTANT_APP_PREFIX):
                            has_assistant_postfix = True
                    if not (
                        depl_status.status == "HEALTHY"
                        and depl_status.status_trigger == "CONFIG_UPDATE_COMPLETED"
                        and depl_status.replica_states[ReplicaState.RUNNING] > 0
                    ):
                        has_one_deployment_not_healthy = True
                        break

                    if has_assistant_postfix and not has_one_deployment_not_healthy:
                        assistants.append(name)
                    elif has_assistant_postfix:
                        logger.info(
                            f"Detected assistant app '{name}' but it's not healthy."
                    )
            return assistants
        
            
logger.debug(
f"assistant_interface module initialized after {time.time()-start} seconds."
)