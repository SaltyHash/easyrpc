"""
TODO: Write this documentation header.

TODO: Offer more intelligent caching methods, like LRU.
TODO: Support TLS.
TODO: Allow persistent client connections (though this may be difficult with how compression works right now).
"""

import logging
import pickle
import socket
import socketserver
import subprocess
from enum import IntEnum
from gzip import GzipFile
from threading import RLock
from typing import Any, Callable, Dict, Iterable, Tuple, Type, Union

__all__ = [
    # Functions
    'cache',
    'serve_forever',

    # Classes
    'BadMessageVersionError',
    'BadRequestError',
    'BaseRequestHandler',
    'Client',
    'EasyRPCError',
    'LockingDict',
    'OSCommandRequestHandler',
    'Server'
]

'''
-- RPC Message Specification (Version 0) --

Bit indices in a byte are left-to-right, like so: 01234567

Request Header:
    The Request Header is 1 byte long.
    Bits 0-3: Version, set to 0000.
    Bits 4-6: Unused.
    Bit    7: Request body is gzip-compressed (1) or not compressed (0).

Request Body:
    The Request Header is immediately followed by the pickled Request Body, which is a
    3-tuple of (function-name: str, (positional-arg, ...), {keyword-arg, ...}). The body
    may be gzip-compressed.

Response Header:
    The Response Header is 1 byte long.
    Bits 0-3: Version, set to 0000.
    Bit  4-5: Status.
              00: Function executed normally.
              01: Received a bad request.
              10: An exception occurred while executing the function.
    Bits   6: Unused.
    Bit    7: Response body is gzip-compressed (1) or not compressed (0).

Response Body:
    The Response Header is immediately followed by the pickled Response Body.
    If the function executed normally, then this is just whatever was returned by the function.
    If an exception occurred, then this is the exception that was raised.
    The body may be gzip-compressed.
'''

'''---------- CONSTANTS ----------'''

COMPRESS_LEVEL: int = 1  # 1 (fastest & largest) - 9 (slowest & smallest)
COMPRESS_THRESHOLD: int = 1459  # 1460 (TCP/IP MSS) - 1 (message header length) = 1459
DEFAULT_PORT: int = 42069
MESSAGE_VERSION: int = 0

'''---------- PRIVATE FUNCTIONS ----------'''


def _load_pickle(file_obj, is_compressed):
    if is_compressed:
        with GzipFile(fileobj=file_obj) as gzip_file:
            return pickle.load(gzip_file, encoding='bytes')
    else:
        return pickle.load(file_obj, encoding='bytes')


'''---------- PUBLIC FUNCTIONS ----------'''


def cache(func):
    """
    A decorator for RPC functions that will cache the results on a per-server basis.

    The arguments given to the function must be hashable.

    The cache is maintained forever, but may be cleared using "server.cache.clear()".
    """

    def wrapper(*args, **kwargs):
        server = args[0].server

        key = (func.__name__, args[1:], tuple(kwargs.items()))

        with server.cache:
            try:
                return server.cache[key]
            except KeyError:
                result = func(*args, **kwargs)
                server.cache[key] = result
                return result

    return wrapper


def serve_forever(*args, **kwargs):
    """Convenience function for starting a server. All arguments are passed to the Server constructor."""

    with Server(*args, **kwargs) as server:
        server.serve_forever()


'''---------- EXCEPTIONS ----------'''


class EasyRPCError(Exception):
    """Base class for all easyrpc errors."""


class BadMessageVersionError(EasyRPCError):
    """Raised when a request or response with an unsupported version is received."""

    def __init__(self, bad_version: int) -> None:
        super().__init__(f'Expected message version {MESSAGE_VERSION}; received version {bad_version}.')


class BadRequestError(EasyRPCError):
    """Raised when there is a problem with the client's request."""

    def __init__(self) -> None:
        super().__init__("There was a problem with the client's request.")


'''---------- PRIVATE CLASSES ----------'''


class _ResponseStatus(IntEnum):
    SUCCESS = 0x00
    BAD_REQUEST = 0x04
    EXCEPTION_OCCURRED = 0x08


class _MessageFile:
    def __init__(self, out_file, threshold: int = COMPRESS_THRESHOLD, compress_level: int = COMPRESS_LEVEL) -> None:
        self.out_file = out_file
        self.threshold = threshold
        self.compress_level = compress_level

        self.buffer = bytearray()
        self.compress_file = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.compress_file:
            self.compress_file.__exit__(exc_type, exc_val, exc_tb)
        else:
            self.write_header()
            self.out_file.write(self.buffer)

        self.out_file.flush()

    def write(self, data: bytes):
        if self.compress_file:
            self.compress_file.write(data)
        else:
            if len(self.buffer) + len(data) > self.threshold:
                self.compress_file = True
                self.write_header()
                self.compress_file = GzipFile(mode='wb', fileobj=self.out_file, compresslevel=self.compress_level)
                self.compress_file.write(self.buffer)
                del self.buffer
                self.compress_file.write(data)
            else:
                self.buffer += data

    def write_header(self):
        raise NotImplementedError


class _RequestFile(_MessageFile):
    def write_header(self):
        header = MESSAGE_VERSION << 4

        if self.compress_file:
            header |= 0x01

        self.out_file.write(bytes((header,)))


class _ResponseFile(_MessageFile):
    def __init__(
            self,
            status: _ResponseStatus,
            out_file,
            threshold: int = COMPRESS_THRESHOLD,
            compress_level: int = COMPRESS_LEVEL
    ) -> None:
        super().__init__(out_file, threshold, compress_level)

        self.status = status

    def write_header(self) -> None:
        header = MESSAGE_VERSION << 4

        header |= self.status

        if self.compress_file:
            header |= 0x01

        self.out_file.write(bytes((header,)))


'''---------- PUBLIC CLASSES ----------'''


class BaseRequestHandler(socketserver.StreamRequestHandler):
    def __init__(self, request, client_address, server):
        try:
            client_hostname = socket.gethostbyaddr(client_address[0])[0]
        except socket.herror:
            client_hostname = client_address[0]

        suffix = f' - {client_hostname}:{client_address[1]}'
        self.log = logging.getLogger(server.log.name + suffix)

        super().__init__(request, client_address, server)

    def setup(self) -> None:
        self.log.debug('Client connected.')
        super().setup()

    def handle(self) -> None:
        try:
            self.send_response(*self._handle())
        except Exception as e:
            # Log any uncaught exceptions
            self.log.exception(e)

    def _handle(self) -> Tuple[_ResponseStatus, Any]:
        # Get the function and arguments
        try:
            function_name, args, kwargs = self.get_request()

            if self.log.isEnabledFor(logging.DEBUG):
                arg_str = ', '.join(repr(arg) for arg in args)
                if arg_str and kwargs:
                    arg_str += ', '
                arg_str += ', '.join(f'{key}={repr(value)}' for key, value in kwargs.items())

                self.log.info(f'{function_name}({arg_str})')
            else:
                self.log.info(f'{function_name}(...)')

            function = self.get_function(function_name)
        except Exception as e:
            self.log.error('Exception caused by bad request:')
            self.log.exception(e, exc_info=self.log.isEnabledFor(logging.DEBUG))
            return _ResponseStatus.BAD_REQUEST, e

        # Run the function
        try:
            result = function(*args, **kwargs)
        except Exception as e:
            self.log.error('Exception occurred while executing the function:')
            self.log.exception(e)
            return _ResponseStatus.EXCEPTION_OCCURRED, e

        # Return the response
        return _ResponseStatus.SUCCESS, result

    def finish(self) -> None:
        super().finish()
        self.log.debug('Done.')

    def get_function(self, function_name: str) -> Callable:
        return getattr(self, f'rpc_{function_name}')

    def get_request(self) -> Tuple[str, Tuple[Any], Dict[str, Any]]:
        """
        :return: (function-name: str, args: tuple, kwargs: dict)
        """

        _, is_compressed = self.parse_request_header()

        return _load_pickle(self.rfile, is_compressed)

    def parse_request_header(self) -> Tuple[int, bool]:
        """
        :return: (version: int, is_compressed: bool)
        :raises BadMessageVersionError: Message version is invalid.
        """

        header = self.rfile.read(1)[0]

        # Version is in bits 0-3
        version = (header & 0xF0) >> 4
        if version != MESSAGE_VERSION:
            raise BadMessageVersionError(version)

        # Compression is bit 7
        is_compressed = bool(header & 0x01)

        self.log.debug(f'Request Header: version={version}, is_compressed={is_compressed}.')

        return version, is_compressed

    def send_response(self, status: _ResponseStatus, response: Any) -> None:
        if self.log.level <= logging.DEBUG:
            self.log.debug(f'Sending: {repr(response)}')

        with _ResponseFile(status, self.wfile) as response_file:
            pickle.dump(response, response_file, protocol=pickle.HIGHEST_PROTOCOL)

    def rpc_get_functions(self, human_readable: bool = True) -> Union[str, Dict[str, str]]:
        """
        Returns the RPC functions supported by the server.

        :param human_readable: Defaults to True.
        :return: The server's RPC functions and their docstrings, as a printable string if human_readable is True, or a
        dictionary mapping the function names to their docstrings if human_readable is False.
        """

        functions = {}
        for function_name in dir(self):
            # Ignore non-RPC functions
            if not function_name.startswith('rpc_'):
                continue

            function = getattr(self, function_name)

            functions[function_name[4:]] = function.__doc__

        if human_readable:
            lines = []
            for function in sorted(functions.keys()):
                lines.append(f'{function}(...)')

                docs = functions[function]
                if docs:
                    lines.append(docs)

                lines.append('')

            return '\n'.join(lines)
        else:
            return functions


class OSCommandRequestHandler(BaseRequestHandler):
    """Implements an RPC function "run_command", which runs arbitrary OS commands."""

    def rpc_run_command(
            self,
            *args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            **kwargs
    ) -> subprocess.CompletedProcess:
        """
        Runs an OS command and returns the resulting CompletedProcess instance,
        which contains the args, return code, and (by default) the stdout/stderr
        read in text mode.

        :param args: Positional arguments passed to subprocess.run().
        :param stdout: Defaults to subprocess.PIPE.
        :param stderr: Defaults to subprocess.PIPE.
        :param text: Defaults to True.
        :param kwargs: Keyword arguments passed to subprocess.run().
        :return: The CompletedProcess instance returned by subprocess.run().
        """

        result = subprocess.run(*args, stdout=stdout, stderr=stderr, text=text, **kwargs)

        self.log.info(f'Ran OS command: {result.args} -> {result.returncode}')

        if self.log.isEnabledFor(logging.DEBUG):
            # Log stdout
            if hasattr(result, 'stdout') and result.stdout:
                self.log.debug(f'- stdout:\n{result.stdout}')
            else:
                self.log.debug('- stdout: [None]')

            # Log stderr
            if hasattr(result, 'stderr') and result.stderr:
                self.log.debug(f'- stderr:\n{result.stderr}')
            else:
                self.log.debug('- stderr: [None]')

        return result


class LockingDict(dict):
    """Works just like a dict, but it can be locked using a `with` block."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.__lock = RLock()

    def __enter__(self):
        self.__lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__lock.release()


class Server(socketserver.ThreadingTCPServer):
    def __init__(self, request_handler_class: Type[BaseRequestHandler], host: str = '', port: int = DEFAULT_PORT):
        self._log = logging.getLogger(f'{self.__class__.__name__}@{host if host else "*"}:{port}')

        super().__init__((host, port), request_handler_class, bind_and_activate=True)

        self._cache = LockingDict()
        self._state = LockingDict()

    @property
    def cache(self) -> LockingDict:
        return self._cache

    @property
    def log(self) -> logging.Logger:
        return self._log

    @property
    def state(self) -> LockingDict:
        """
        Returns the server's state dictionary (an instance of LockingDict).

        By default, the state dictionary is unused; it is up to the request handler to use it, if desired.
        It is merely supplied as a convenience.

        In order to maintain thread safety while doing operations involving the state,
        the request handler should lock the state using a `with` block, like so:

            with self.server.state as state:
                # Do things with state
                state['foo'] = 'bar'

        :return: The server's state dictionary.
        """

        return self._state

    def server_activate(self):
        self.log.info(f'Starting...')
        super().server_activate()
        self.log.info(f'Done.')

    def server_close(self):
        self.log.info(f'Shutting down...')
        super().server_close()
        self.log.info(f'Done.')


class Client:
    def __init__(self, host: str, port: int = DEFAULT_PORT):
        self.host = host
        self.port = port

    def __dir__(self) -> Iterable[str]:
        dir_ = list(super().__dir__())
        dir_.extend(self.get_functions().keys())
        return dir_

    def __getattr__(self, item: str) -> Any:
        def function(*args, **kwargs):
            return self._rpc(item, *args, **kwargs)

        # function.__doc__ = f'{item}(...)\n\nTODO: This.'

        return function

    def _rpc(self, function, *args, **kwargs) -> object:
        message = (function, args, kwargs)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.host, self.port))

            with sock.makefile('wb') as sock_file, _RequestFile(sock_file) as request_file:
                pickle.dump(message, request_file, protocol=pickle.HIGHEST_PROTOCOL)

            # Without shutting down the sending part of the socket, if the data is
            # compressed, then for some reason the server's GzipFile seems to hang
            # until the socket is closed. I suspect it should not work this way --
            # the pickle.load(gzip_file) call should just read until it has everything
            # it needs, and the GzipFile should just decode as needed. It may have to do
            # with gzip block sizes -- maybe GzipFile is waiting for more block data
            # that is not coming.
            sock.shutdown(socket.SHUT_WR)

            with sock.makefile('rb') as sock_file:
                _, status, is_compressed = self._parse_response_header(sock_file)

                result = _load_pickle(sock_file, is_compressed)

        if status == _ResponseStatus.SUCCESS:
            return result
        if status == _ResponseStatus.BAD_REQUEST:
            raise BadRequestError from result
        else:
            raise result

    @staticmethod
    def _get_request_header(is_compressed: bool) -> bytes:
        header = MESSAGE_VERSION << 4 | is_compressed

        return bytes((header,))

    @staticmethod
    def _parse_response_header(sock_file) -> Tuple[int, _ResponseStatus, bool]:
        """
        :return: (version, status, is_compressed)
        :raises ValueError: Message version is invalid.
        """

        header = sock_file.read(1)[0]

        # Version is in bits 0-3
        version = (header & 0xF0) >> 4
        if version != MESSAGE_VERSION:
            raise BadMessageVersionError(version)

        # Status is in bits 4-5
        if header & _ResponseStatus.BAD_REQUEST:
            status = _ResponseStatus.BAD_REQUEST
        elif header & _ResponseStatus.EXCEPTION_OCCURRED:
            status = _ResponseStatus.EXCEPTION_OCCURRED
        else:
            status = _ResponseStatus.SUCCESS

        # Compression is bit 7
        is_compressed = bool(header & 0x01)

        return version, status, is_compressed


if __name__ == '__main__':
    def test_client():
        client = Client('localhost')

        functions = client.get_functions()

        print('--- Functions ---\n')
        print(functions)

        import pdb
        pdb.set_trace(header='Use variable "client" to interact with the server.')


    def test_server():
        logging.basicConfig(
            level=logging.DEBUG,
            style='{',
            format='{asctime} - {name} - {levelname}: {message}',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        serve_forever(OSCommandRequestHandler, host='localhost')


    def main():
        import sys

        try:
            option = sys.argv[1].strip().lower()
        except IndexError:
            option = None

        if option == 'client':
            test_client()
        elif option == 'server':
            test_server()
        else:
            print(f'Usage: {sys.argv[0]} <client|server>')
            sys.exit(1)


    main()
