from abc import abstractmethod, ABCMeta

from .enums import PNStatusCategory
from .models.consumer.common import PNStatus
from .models.server.subscribe import SubscribeEnvelope
from .dtos import SubscribeOperation, UnsubscribeOperation
from .callbacks import SubscribeCallback
from .models.subscription_item import SubscriptionItem


class PublishSequenceManager(object):
    def __init__(self, provided_max_sequence):
        self.max_sequence = provided_max_sequence
        self.next_sequence = 0

    # TODO: should be thread-safe
    def get_next_sequence(self):
        if self.max_sequence == self.next_sequence:
            self.next_sequence = 1
        else:
            self.next_sequence += 1

        return self.next_sequence


class StateManager(object):
    def __init__(self):
        self._channels = {}
        self._groups = {}
        self._presence_channels = {}
        self._presence_groups = {}

    def is_empty(self):
        return len(self._channels) == 0 and len(self._groups) == 0 and\
               len(self._presence_channels) == 0 and len(self._presence_groups) == 0

    def subscribed_to_the_only_channel(self):
        return len(self._channels) == 1 and len(self._groups) == 0 and\
               len(self._presence_channels) == 0 and len(self._presence_groups) == 0

    def prepare_channel_list(self, include_presence):
        return StateManager._prepare_membership_list(
            self._channels, self._presence_channels, include_presence)

    def prepare_channel_group_list(self, include_presence):
        return StateManager._prepare_membership_list(
            self._groups, self._presence_groups, include_presence)

    def adapt_subscribe_builder(self, subscribe_operation):
        for channel in subscribe_operation.channels:
            self._channels[channel] = SubscriptionItem(name=channel)

            if subscribe_operation.presence_enabled:
                self._presence_channels[channel] = SubscriptionItem(name=channel)

        for group in subscribe_operation.channel_groups:
            self._groups[group] = SubscriptionItem(name=group)

            if subscribe_operation.presence_enabled:
                self._presence_groups[group] = SubscriptionItem(name=group)

    def adapt_unsubscribe_builder(self, unsubscribe_operation):
        for channel in unsubscribe_operation.channels:
            self._channels.pop(channel, None)
            if channel in self._presence_channels:
                self._presence_channels.pop(channel, None)

        for group in unsubscribe_operation.channel_groups:
            self._groups.pop(group)
            if group in self._presence_groups:
                self._presence_groups.pop(group)

    def state_payload(self):
        state = {}

        for channel in self._channels.values():
            if channel.state is not None:
                state[channel.name] = channel.state

        for group in self._groups.values():
            if group.state is not None:
                state[group.name] = group.state

        return state

    @staticmethod
    def _prepare_membership_list(data_storage, presence_storage, include_presence):
        response = []

        for item in data_storage.values():
            response.append(item.name)

        if include_presence:
            for item in presence_storage.values():
                response.append(item.name + "-pnpres")

        return response


class ListenerManager(object):
    def __init__(self, pubnub_instance):
        self._pubnub = pubnub_instance
        self._listeners = []

    def add_listener(self, listener):
        assert isinstance(listener, SubscribeCallback)
        self._listeners.append(listener)

    def remove_listener(self, listener):
        assert isinstance(listener, SubscribeCallback)
        self._listeners.remove(listener)

    def announce_status(self, status):
        for callback in self._listeners:
            callback.status(self._pubnub, status)

    def announce_message(self, message):
        for callback in self._listeners:
            callback.message(self._pubnub, message)

    def announce_presence(self, presence):
        for callback in self._listeners:
            callback.presence(self._pubnub, presence)


class SubscriptionManager(object):
    __metaclass__ = ABCMeta

    HEARTBEAT_INTERVAL_MULTIPLIER = 1000

    def __init__(self, pubnub_instance):
        self._pubnub = pubnub_instance
        self._subscription_status_announced = False

        self._subscription_state = StateManager()
        self._listener_manager = ListenerManager(self._pubnub)
        self._timetoken = int(0)
        self._region = None

        self._should_stop = False

        self._subscribe_request_task = None
        self._heartbeat_call = None

        self._start_worker()

    @abstractmethod
    def _start_worker(self):
        pass

    @abstractmethod
    def _set_consumer_event(self):
        pass

    @abstractmethod
    def _message_queue_put(self, message):
        pass

    @abstractmethod
    def _start_subscribe_loop(self):
        pass

    @abstractmethod
    def _stop_subscribe_loop(self):
        pass

    @abstractmethod
    def _stop_heartbeat_timer(self):
        pass

    @abstractmethod
    def _perform_heartbeat_loop(self):
        pass

    @abstractmethod
    def _send_leave(self, unsubscribe_operation):
        pass

    def add_listener(self, listener):
        self._listener_manager.add_listener(listener)

    def adapt_subscribe_builder(self, subscribe_operation):
        assert isinstance(subscribe_operation, SubscribeOperation)
        self._subscription_state.adapt_subscribe_builder(subscribe_operation)
        self._subscription_status_announced = False

        if subscribe_operation.timetoken is not None:
            self._timetoken = subscribe_operation.timetoken

        self.reconnect()

    def adapt_unsubscribe_builder(self, unsubscribe_operation):
        assert isinstance(unsubscribe_operation, UnsubscribeOperation)

        self._subscription_state.adapt_unsubscribe_builder(unsubscribe_operation)

        self._send_leave(unsubscribe_operation)

        if self._subscription_state.is_empty():
            self._region = None
            self._timetoken = 0
        self.reconnect()

    @abstractmethod
    def reconnect(self):
        pass

    def stop(self):
        self._should_stop = True
        self._stop_subscribe_loop()
        self._stop_heartbeat_timer()
        self._set_consumer_event()

    def _handle_endpoint_call(self, raw_result, status):
        assert isinstance(status, PNStatus)

        if not self._subscription_status_announced:
            pn_status = PNStatus()
            pn_status.category = PNStatusCategory.PNConnectedCategory
            pn_status.status_code = status.status_code
            pn_status.auth_key = status.auth_key
            pn_status.operation = status.operation
            pn_status.client_request = status.client_request
            pn_status.origin = status.origin
            pn_status.tls_enabled = status.tls_enabled

            self._subscription_status_announced = True
            self._listener_manager.announce_status(pn_status)

        result = SubscribeEnvelope.from_json(raw_result)
        only_channel = self._subscription_state.subscribed_to_the_only_channel()
        if result.messages is not None and len(result.messages) > 0:
            for message in result.messages:
                if only_channel:
                    message.only_channel_subscription = True
                self._message_queue_put(message)

        # REVIEW: is int compatible with long for Python 2
        self._timetoken = int(result.metadata.timetoken)
        self._region = int(result.metadata.region)

    # TODO: make abstract
    def _register_heartbeat_timer(self):
        self._stop_heartbeat_timer()
