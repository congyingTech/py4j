from inspect import ismethod
from threading import RLock

from py4j.compat import range


def make_id(func):
    if ismethod(func):
        return (id(func.__self__), id(func.__func__))
    return id(func)

NONE_ID = make_id(None)


class Signal(object):
    """Basic signal class that can register receivers (listeners) and dispatch
    events to these receivers.

    As opposed to many signals libraries, receivers are not stored as weak
    references, so it is us to the client application to unregister them.

    Greatly inspired from Django Signals:
    https://github.com/django/django/blob/master/django/dispatch/dispatcher.py
    """

    def __init__(self):
        self.lock = RLock()
        self.receivers = []

    def connect(self, receiver, unique_id=None):
        """Registers a receiver for this signal.

        The receiver must be a callable (e.g., function or instance method)
        that accepts named arguments (i.e., ``**kwargs``).

        In case that the connect method might be called multiple time, it is
        best to provide the receiver with a unique id to make sure that the
        receiver is not registered more than once.

        :param receiver: The callable that will receive the signal.
        :param unique_id: The unique id of the callable to make sure it is not
            registered more than once. Optional.
        """
        full_id = self._get_id(receiver, unique_id)

        with self.lock:
            for receiver_id, _ in self.receivers:
                if receiver_id == full_id:
                    break
            else:
                self.receivers.append((full_id, receiver))

    def disconnect(self, receiver, unique_id=None):
        """Unregisters a receiver for this signal.

        :param receiver: The callable that was registered to receive the
            signal.
        :param unique_id: The unique id of the callable if it was provided.
            Optional.
        :return: True if the receiver was found and disconnected. False
            otherwise.
        :rtype: bool
        """
        full_id = self._get_id(receiver, unique_id)
        disconnected = False

        with self.lock:
            for index in range(len(self.receivers)):
                temp_id = self.receivers[index][0]
                if temp_id == full_id:
                    del self.receivers[index]
                    disconnected = True

        return disconnected

    def send(self, **kwargs):
        """
        """
        pass

    def _get_id(self, receiver, unique_id):
        if unique_id:
            full_id = (make_id(receiver), unique_id)
        else:
            full_id = (make_id(receiver), NONE_ID)
        return full_id
