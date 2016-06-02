import logging
from Queue import Queue, Empty


from .models.consumer.pubsub import PNPresenceEventResult, PNMessageResult
from .models.server.subscribe import SubscribeMessage, PresenceEnvelope
# from .pubnub_core import PubNubCore
# from .managers import ListenerManager

logger = logging.getLogger("pubnub")


class SubscribeMessageWorker(object):
    def __init__(self, pubnub_instnace, listener_manager_instance, queue_instance, event):
        # assert isinstance(pubnub_instnace, PubNubCore)
        # assert isinstance(listener_manager_instance, ListenerManager)
        assert isinstance(queue_instance, Queue)

        self._pubnub = pubnub_instnace
        self._listener_manager = listener_manager_instance
        self._queue = queue_instance
        self._is_running = None
        self._event = event

    def run(self):
        print("run SubscribeMessageWorker")
        self._take_message()

    def _take_message(self):
        while not self._event.isSet():
            try:
                # TODO: get rid of 1s timeout
                msg = self._queue.get(True, 1)
                if msg is not None:
                    self._process_incoming_payload(msg)
                self._queue.task_done()
            except Empty:
                pass
            except Exception as e:
                print("exception", e)
                self._event.set()
                logger.warn("take message interrupted: %s" % str(e))

    def _process_message(self, message_input):
        if self._pubnub.config.cipher_key is None:
            return message_input
        else:
            return "TODO: implement cipher decoding"

    def _process_incoming_payload(self, message):
        assert isinstance(message, SubscribeMessage)

        channel = message.channel
        subscription_match = message.subscription_match
        publish_meta_data = message.publish_metadata

        if "-pnpres" in message.channel:
            presence_payload = PresenceEnvelope.from_json_payload(message.payload)
            pn_presence_event_result = PNPresenceEventResult(
                event=presence_payload.action,
                actual_channel=(channel if subscription_match is not None else None),
                subscribed_channel=(subscription_match if subscription_match is not None else channel),
                timetoken=publish_meta_data.publish_timetoken,
                occupancy=presence_payload.occupancy,
                uuid=presence_payload.uuid,
                timestamp=presence_payload.timestamp
            )
            self._listener_manager.announce_presence(pn_presence_event_result)
        else:
            extracted_message = self._process_message(message.payload)

            if extracted_message is None:
                logger.debug("unable to parse payload on #processIncomingMessages")

            pn_message_result = PNMessageResult(
                message=extracted_message,
                actual_channel=(channel if subscription_match is not None else None),
                subscribed_channel=(subscription_match if subscription_match is not None else channel),
                timetoken=publish_meta_data.publish_timetoken
            )
            self._listener_manager.announce_message(pn_message_result)
