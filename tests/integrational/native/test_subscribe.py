import logging
import unittest

# import pydevd as pydevd

import pubnub
from pubnub.callbacks import SubscribeCallback
from pubnub.exceptions import PubNubException
from pubnub.pubnub import PubNub
from tests.helper import pnconf, CountDownLatch

pubnub.set_stream_logger('pubnub', logging.DEBUG)
# pydevd.settrace('192.168.163.1', port=8899, stdoutToServer=True, stderrToServer=True)


class TestPubNubSubscribe(unittest.TestCase):
    # @vcr.use_cassette('integrational/fixtures/publish/publish_string_get.yaml',
    #                   filter_query_parameters=['uuid'])
    def test_subscribe_latched(self):
        pubnub = PubNub(pnconf)
        latch = CountDownLatch()

        class MyCallback(SubscribeCallback):
            def __init__(self, l):
                super(MyCallback, self).__init__()

                assert isinstance(l, CountDownLatch)
                self._latch = l

            def status(self, p, status):
                print("@status changed")
                res = p.publish().channel('ch1').message('hey').sync()
                print(res)
                self._latch.count_down()
                # event.set()

            def presence(self, p, presence):
                print("@presence event")
                self._latch.count_down()
                # event.set()

            def message(self, p, message):
                self._latch.count_down()
                print("@new message")
                # event.set()

        try:
            callback = MyCallback(latch)
            pubnub.add_listener(callback)
            pubnub.subscribe() \
                .channels(["ch1", "ch2"]) \
                .execute()

            latch.await(60)

            assert latch.done
            pubnub.stop()
        except PubNubException as e:
            self.fail(e)
