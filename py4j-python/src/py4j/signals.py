from inspect import ismethod
from threading import Lock

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
        self.lock = Lock()
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
                    break

        return disconnected

    def send(self, sender, **params):
        """Sends the signal to all connected receivers.

        If a receiver raises an error, the error is propagated back and
        interrupts the sending processing. It is thus possible that not all
        receivers will receive the signal.

        :param: named parameters to send to the receivers.
        :param: the sender of the signal. Optional.
        :return: List of (receiver, response) from receivers.
        :rtype: list
        """
        responses = []
        for receiver in self._get_receivers():
            response = receiver(signal=self, sender=sender, **params)
            responses.append((receiver, response))
        return responses

    def _get_receivers(self):
        """Internal method that may in the future resolve weak references or
        perform other work such as identifying dead receivers.
        """
        with self.lock:
            receivers = [receiver[1] for receiver in self.receivers]
        return receivers

    def _get_id(self, receiver, unique_id):
        if unique_id:
            full_id = (make_id(receiver), unique_id)
        else:
            full_id = (make_id(receiver), NONE_ID)
        return full_id
