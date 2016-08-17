import logging
import asyncio
import pytest
import pubnub as pn

from pubnub.models.consumer.pubsub import PNMessageResult
from pubnub.pubnub_asyncio import PubNubAsyncio, AsyncioEnvelope, SubscribeListener
from tests.helper import pnconf_sub_copy, pnconf_enc_sub_copy
from tests.integrational.vcr_helper import pn_vcr, get_sleeper

pn.set_stream_logger('pubnub', logging.DEBUG)


@pn_vcr.use_cassette('tests/integrational/fixtures/asyncio/subscription/sub_unsub.yaml',
                     filter_query_parameters=['uuid'])
@pytest.mark.asyncio
def test_subscribe_unsubscribe(event_loop):
    channel = "test-subscribe-asyncio-ch"

    pubnub = PubNubAsyncio(pnconf_sub_copy(), custom_event_loop=event_loop)

    callback = SubscribeListener()
    pubnub.add_listener(callback)
    pubnub.subscribe().channels(channel).execute()

    yield from callback.wait_for_connect()

    pubnub.unsubscribe().channels(channel).execute()
    yield from callback.wait_for_disconnect()

    pubnub.stop()


@pn_vcr.use_cassette('tests/integrational/fixtures/asyncio/subscription/sub_pub_unsub.yaml')
@pytest.mark.asyncio
def test_subscribe_publish_unsubscribe(event_loop):
    pubnub = PubNubAsyncio(pnconf_sub_copy(), custom_event_loop=event_loop)
    pubnub.config.uuid = 'test-subscribe-asyncio-uuid'

    callback = SubscribeListener()
    channel = "test-subscribe-asyncio-ch"
    message = "hey"
    pubnub.add_listener(callback)
    pubnub.subscribe().channels(channel).execute()

    yield from callback.wait_for_connect()

    publish_future = asyncio.ensure_future(pubnub.publish().channel(channel).message(message).future())
    subscribe_message_future = asyncio.ensure_future(callback.wait_for_message_on(channel))

    yield from asyncio.wait([
        publish_future,
        subscribe_message_future
    ])

    publish_envelope = publish_future.result()
    subscribe_envelope = subscribe_message_future.result()

    assert isinstance(subscribe_envelope, PNMessageResult)
    assert subscribe_envelope.actual_channel == channel
    assert subscribe_envelope.subscribed_channel == channel
    assert subscribe_envelope.message == message
    assert subscribe_envelope.timetoken > 0

    assert isinstance(publish_envelope, AsyncioEnvelope)
    assert publish_envelope.result.timetoken > 0
    assert publish_envelope.status.original_response[0] == 1

    pubnub.unsubscribe().channels(channel).execute()
    yield from callback.wait_for_disconnect()

    pubnub.stop()


@pn_vcr.use_cassette('tests/integrational/fixtures/asyncio/subscription/sub_pub_unsub_enc.yaml')
@pytest.mark.asyncio
def test_encrypted_subscribe_publish_unsubscribe(event_loop):
    pubnub = PubNubAsyncio(pnconf_enc_sub_copy(), custom_event_loop=event_loop)
    pubnub.config.uuid = 'test-subscribe-asyncio-uuid'

    callback = SubscribeListener()
    channel = "test-subscribe-asyncio-ch"
    message = "hey"
    pubnub.add_listener(callback)
    pubnub.subscribe().channels(channel).execute()

    yield from callback.wait_for_connect()

    publish_future = asyncio.ensure_future(pubnub.publish().channel(channel).message(message).future())
    subscribe_message_future = asyncio.ensure_future(callback.wait_for_message_on(channel))

    yield from asyncio.wait([
        publish_future,
        subscribe_message_future
    ])

    publish_envelope = publish_future.result()
    subscribe_envelope = subscribe_message_future.result()

    assert isinstance(subscribe_envelope, PNMessageResult)
    assert subscribe_envelope.actual_channel == channel
    assert subscribe_envelope.subscribed_channel == channel
    assert subscribe_envelope.message == message
    assert subscribe_envelope.timetoken > 0

    assert isinstance(publish_envelope, AsyncioEnvelope)
    assert publish_envelope.result.timetoken > 0
    assert publish_envelope.status.original_response[0] == 1

    pubnub.unsubscribe().channels(channel).execute()
    yield from callback.wait_for_disconnect()

    pubnub.stop()


@pn_vcr.use_cassette('tests/integrational/fixtures/asyncio/subscription/join_leave.yaml')
@pytest.mark.asyncio
def test_join_leave_blah(event_loop):
    channel = "test-subscribe-asyncio-join-leave-ch"
    presence_channel = "%s-pnpres" % channel

    pubnub = PubNubAsyncio(pnconf_sub_copy(), custom_event_loop=event_loop)
    pubnub_listener = PubNubAsyncio(pnconf_sub_copy(), custom_event_loop=event_loop)

    pubnub.config.uuid = "test-subscribe-asyncio-messenger"
    pubnub_listener.config.uuid = "test-subscribe-asyncio-listener"

    callback_presence = SubscribeListener()
    callback_messages = SubscribeListener()

    pubnub_listener.add_listener(callback_presence)
    pubnub_listener.subscribe().channels(channel).with_presence().execute()

    yield from callback_presence.wait_for_connect()

    envelope = yield from callback_presence.wait_for_presence_on(channel)
    assert envelope.actual_channel == presence_channel
    assert envelope.event == 'join'
    assert envelope.uuid == pubnub_listener.uuid

    pubnub.add_listener(callback_messages)
    pubnub.subscribe().channels(channel).execute()
    yield from callback_messages.wait_for_connect()

    envelope = yield from callback_presence.wait_for_presence_on(channel)
    assert envelope.actual_channel == presence_channel
    assert envelope.event == 'join'
    assert envelope.uuid == pubnub.uuid

    pubnub.unsubscribe().channels(channel).execute()
    yield from callback_messages.wait_for_disconnect()

    envelope = yield from callback_presence.wait_for_presence_on(channel)
    assert envelope.actual_channel == presence_channel
    assert envelope.event == 'leave'
    assert envelope.uuid == pubnub.uuid

    pubnub_listener.unsubscribe().channels(channel).execute()
    yield from callback_presence.wait_for_disconnect()

    pubnub.stop()
    pubnub_listener.stop()


@get_sleeper('tests/integrational/fixtures/asyncio/subscription/cg_sub_unsub.yaml')
@pn_vcr.use_cassette('tests/integrational/fixtures/asyncio/subscription/cg_sub_unsub.yaml',
                     filter_query_parameters=['uuid'])
@pytest.mark.asyncio
def test_cg_subscribe_unsubscribe(event_loop, sleeper=asyncio.sleep):
    ch = "test-subscribe-asyncio-channel"
    gr = "test-subscribe-asyncio-group"

    pubnub = PubNubAsyncio(pnconf_sub_copy(), custom_event_loop=event_loop)

    envelope = yield from pubnub.add_channel_to_channel_group().channel_group(gr).channels(ch).future()
    assert envelope.status.original_response['status'] == 200

    yield from sleeper(3)

    callback_messages = SubscribeListener()
    pubnub.add_listener(callback_messages)
    pubnub.subscribe().channel_groups(gr).execute()
    yield from callback_messages.wait_for_connect()

    pubnub.unsubscribe().channel_groups(gr).execute()
    yield from callback_messages.wait_for_disconnect()

    envelope = yield from pubnub.remove_channel_from_channel_group().channel_group(gr).channels(ch).future()
    assert envelope.status.original_response['status'] == 200

    pubnub.stop()


@get_sleeper('tests/integrational/fixtures/asyncio/subscription/cg_sub_pub_unsub.yaml')
@pn_vcr.use_cassette('tests/integrational/fixtures/asyncio/subscription/cg_sub_pub_unsub.yaml',
                     filter_query_parameters=['uuid'])
@pytest.mark.asyncio
def test_cg_subscribe_publish_unsubscribe(event_loop, sleeper=asyncio.sleep):
    ch = "test-subscribe-asyncio-channel"
    gr = "test-subscribe-asyncio-group"
    message = "hey"

    pubnub = PubNubAsyncio(pnconf_sub_copy(), custom_event_loop=event_loop)

    envelope = yield from pubnub.add_channel_to_channel_group().channel_group(gr).channels(ch).future()
    assert envelope.status.original_response['status'] == 200

    yield from sleeper(1)

    callback_messages = SubscribeListener()
    pubnub.add_listener(callback_messages)
    pubnub.subscribe().channel_groups(gr).execute()
    yield from callback_messages.wait_for_connect()

    subscribe_future = asyncio.ensure_future(callback_messages.wait_for_message_on(ch))
    publish_future = asyncio.ensure_future(pubnub.publish().channel(ch).message(message).future())
    yield from asyncio.wait([subscribe_future, publish_future])

    sub_envelope = subscribe_future.result()
    pub_envelope = publish_future.result()

    assert pub_envelope.status.original_response[0] == 1
    assert pub_envelope.status.original_response[1] == 'Sent'

    assert sub_envelope.actual_channel == ch
    assert sub_envelope.subscribed_channel == gr
    assert sub_envelope.message == message

    pubnub.unsubscribe().channel_groups(gr).execute()
    yield from callback_messages.wait_for_disconnect()

    envelope = yield from pubnub.remove_channel_from_channel_group().channel_group(gr).channels(ch).future()
    assert envelope.status.original_response['status'] == 200

    pubnub.stop()


@get_sleeper('tests/integrational/fixtures/asyncio/subscription/cg_join_leave.yaml')
@pn_vcr.use_cassette('tests/integrational/fixtures/asyncio/subscription/cg_join_leave.yaml')
@pytest.mark.asyncio
def test_cg_join_leave(event_loop, sleeper=asyncio.sleep):
    pubnub = PubNubAsyncio(pnconf_sub_copy(), custom_event_loop=event_loop)
    pubnub_listener = PubNubAsyncio(pnconf_sub_copy(), custom_event_loop=event_loop)

    pubnub.config.uuid = "test-subscribe-asyncio-messenger"
    pubnub_listener.config.uuid = "test-subscribe-asyncio-listener"

    ch = "test-subscribe-asyncio-join-leave-cg-channel"
    gr = "test-subscribe-asyncio-join-leave-cg-group"

    envelope = yield from pubnub.add_channel_to_channel_group().channel_group(gr).channels(ch).future()
    assert envelope.status.original_response['status'] == 200

    yield from sleeper(1)

    callback_messages = SubscribeListener()
    callback_presence = SubscribeListener()

    pubnub_listener.add_listener(callback_presence)
    pubnub_listener.subscribe().channel_groups(gr).with_presence().execute()
    yield from callback_presence.wait_for_connect()
    print("connected #1")

    prs_envelope = yield from callback_presence.wait_for_presence_on(ch)
    print("presence #2")
    assert prs_envelope.event == 'join'
    assert prs_envelope.uuid == pubnub_listener.uuid
    assert prs_envelope.actual_channel == ch + "-pnpres"
    assert prs_envelope.subscribed_channel == gr + "-pnpres"

    pubnub.add_listener(callback_messages)
    pubnub.subscribe().channel_groups(gr).execute()
    print("subscribed to cg #3")

    callback_messages_future = asyncio.ensure_future(callback_messages.wait_for_connect())
    presence_messages_future= asyncio.ensure_future(callback_presence.wait_for_presence_on(ch))
    yield from asyncio.wait([callback_messages_future, presence_messages_future])
    print("connect/presence #4")
    prs_envelope = presence_messages_future.result()

    assert prs_envelope.event == 'join'
    assert prs_envelope.uuid == pubnub.uuid
    assert prs_envelope.actual_channel == ch + "-pnpres"
    assert prs_envelope.subscribed_channel == gr + "-pnpres"

    pubnub.unsubscribe().channel_groups(gr).execute()

    callback_messages_future = asyncio.ensure_future(callback_messages.wait_for_disconnect())
    presence_messages_future = asyncio.ensure_future(callback_presence.wait_for_presence_on(ch))
    yield from asyncio.wait([callback_messages_future, presence_messages_future])
    print("disconnect/presence #5")
    prs_envelope = presence_messages_future.result()

    assert prs_envelope.event == 'leave'
    assert prs_envelope.uuid == pubnub.uuid
    assert prs_envelope.actual_channel == ch + "-pnpres"
    assert prs_envelope.subscribed_channel == gr + "-pnpres"

    pubnub_listener.unsubscribe().channel_groups(gr).execute()
    yield from callback_presence.wait_for_disconnect()
    print("presence disconnect #6")

    envelope = yield from pubnub.remove_channel_from_channel_group().channel_group(gr).channels(ch).future()
    assert envelope.status.original_response['status'] == 200
    print("channel removed #7")

    pubnub.stop()
    pubnub_listener.stop()