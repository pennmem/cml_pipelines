import logging
import threading
from typing import Optional

import zmq


class ZMQLogHandler(logging.Handler):
    """Handler for sending logs over a ZMQ socket for central consumption.

    Parameters
    ----------
    address
        ZMQ address string for the server listening for log records
    ctx
        ZMQ context. If not given, a new one will be created.
    level
        Log level to filter at.

    """
    def __init__(self, address: str,
                 ctx: Optional[zmq.Context] = None,
                 level: int = logging.NOTSET):
        super().__init__(level=level)

        self.ctx = ctx if ctx is not None else zmq.Context()

        self.socket = self.ctx.socket(zmq.PUSH)
        self.socket.connect(address)

    def emit(self, record: logging.LogRecord):
        self.socket.send_pyobj(record)


class BaseLogConsumer(object):
    """A base log consumer.

    Parameters
    ----------
    name
        Name of the logger to use for centralized logging
        (default: "cml").
    address
        ZMQ address string to bind the socket. Default: ``tcp://*:9777``.
    ctx
        ZMQ context. If not given, one will be created when the socket loop is
        started.
    poll_interval
        Polling timeout interval in ms to check for doneness.

    """
    logger = None  # type: logging.Logger

    def __init__(self, name: str = "cml",
                 address: str = "tcp://*:9777",
                 ctx: Optional[zmq.Context] = None,
                 poll_interval: int = 1000):

        self.name = name
        self.address = address
        self.socket_type = zmq.PULL
        self.poll_interval = poll_interval

        self.logger = logging.getLogger(name)
        self._ctx = ctx

    @property
    def ctx(self) -> zmq.Context:
        """Create or get the ZMQ context."""
        if self._ctx is None:
            self._ctx = zmq.Context()
        return self._ctx

    @property
    def port(self) -> int:
        return int(self.address.split(":")[-1])

    def ready(self) -> bool:
        """Check if the socket loop is running."""
        raise NotImplementedError

    def set_ready(self):
        """Indicates that the socket loop is ready to receive messages."""
        raise NotImplementedError

    def done(self) -> bool:
        """Check if it's safe to stop the socket loop."""
        raise NotImplementedError

    def stop(self):
        """Stop the socket loop."""
        raise NotImplementedError

    def run(self):
        socket = self.ctx.socket(self.socket_type)
        socket.bind(self.address)
        self.set_ready()

        while not self.done():
            if not socket.poll(self.poll_interval, zmq.POLLIN):
                continue

            try:
                record = socket.recv_pyobj()
                self.logger.log(record.levelno, record.msg)
            except:
                self.logger.error("Error receiving/logging message",
                                  exc_info=True)


class LogConsumerThread(BaseLogConsumer, threading.Thread):
    """A thread-based log consumer."""
    _done = threading.Event()
    _ready = threading.Event()

    def __init__(self, *args, **kwargs):
        threading.Thread.__init__(self)
        BaseLogConsumer.__init__(self, *args, **kwargs)

    def __enter__(self):
        self.start()
        self._ready.wait()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        self.join()

    def ready(self):
        return self._ready.is_set()

    def set_ready(self):
        self._ready.set()

    def done(self):
        return self._done.is_set()

    def stop(self):
        self._done.set()
