from __future__ import unicode_literals, absolute_import

from unittest import TestCase

from py4j.signals import Signal


class SignalTest(TestCase):

    def setUp(self):
        self.called = [0]
        self.called_kwargs = []

        # For easier access
        called = self.called
        called_kwargs = self.called_kwargs

        def receiver1(signal, sender, **kwargs):
            called[0] += 1
            called_kwargs.append(kwargs)

        class Receiver2(object):

            def receiver2_method(self, signal, sender, **kwargs):
                called[0] += 1
                called_kwargs.append(kwargs)

        def error_receiver3(signal, sender, **kwargs):
            raise Exception("BAD RECEIVER")

        self.alert = Signal()
        self.receiver1 = receiver1
        self.receiver2 = Receiver2()
        self.error_receiver3 = error_receiver3

    def testConnect(self):
        self.alert.connect(self.receiver1)
        self.alert.connect(self.receiver1)
        self.alert.connect(self.receiver2.receiver2_method)
        self.alert.connect(self.receiver2.receiver2_method)
        self.alert.connect(self.receiver1, "foo")
        self.alert.connect(self.receiver1, "bar")
        self.assertEqual(4, len(self.alert.receivers))

    def testDisconnect(self):
        self.testConnect()

        self.assertTrue(self.alert.disconnect(self.receiver1))

        # Already disconnected
        self.assertFalse(self.alert.disconnect(self.receiver1))

        self.assertTrue(self.alert.disconnect(self.receiver1, "foo"))
        self.assertTrue(self.alert.disconnect(self.receiver1, "bar"))
        self.assertTrue(self.alert.disconnect(self.receiver2.receiver2_method))

        self.assertEqual(0, len(self.alert.receivers))

    def testSend(self):
        self.testConnect()
        self.alert.send(SignalTest, param1="foo", param2=3)
        self.assertEqual(4, self.called[0])
        self.assertEqual(4, len(self.called_kwargs))
        self.assertEqual([{"param1": "foo", "param2": 3}] * 4,
                         self.called_kwargs)

    def testSendException(self):
        self.alert.connect(self.receiver1)
        self.alert.connect(self.error_receiver3)
        self.alert.connect(self.receiver1, "foo")

        try:
            self.alert.send(SignalTest, param1="foo", param2=3)
            self.fail()
        except Exception:
            self.assertTrue(True)
        self.assertEqual(1, self.called[0])
        self.assertEqual(1, len(self.called_kwargs))
