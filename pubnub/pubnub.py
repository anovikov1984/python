import logging
import threading
import requests

from .pnconfiguration import PNConfiguration
from .builders import SubscribeBuilder
from .managers import SubscriptionManager
from . import utils
from .structures import RequestOptions, ResponseInfo
from .enums import PNStatusCategory
from .callbacks import SubscribeCallback
from .errors import PNERR_DEFERRED_NOT_IMPLEMENTED, PNERR_SERVER_ERROR, PNERR_CLIENT_ERROR, PNERR_UNKNOWN_ERROR, \
    PNERR_TOO_MANY_REDIRECTS_ERROR, PNERR_CLIENT_TIMEOUT, PNERR_HTTP_ERROR, PNERR_CONNECTION_ERROR
from .exceptions import PubNubException
from .pubnub_core import PubNubCore

logger = logging.getLogger("pubnub")


class PubNub(PubNubCore):
    """PubNub Python API"""

    def __init__(self, config):
        assert isinstance(config, PNConfiguration)
        PubNubCore.__init__(self, config)

        if self.config.enable_subscribe:
            self._subscription_manager = SubscriptionManager(self)

    def subscribe(self):
        return SubscribeBuilder(self._subscription_manager)

    def sdk_platform(self):
        return ""

    def request_sync(self, options):
        res = self.pn_request(self.session, self.config.scheme_and_host(), self.headers, options,
                              self.config.connect_timeout, self.config.non_subscribe_request_timeout)

        # http error
        if res.status_code != requests.codes.ok:
            if res.text is None:
                text = "N/A"
            else:
                text = res.text

            if res.status_code >= 500:
                err = PNERR_SERVER_ERROR
            else:
                err = PNERR_CLIENT_ERROR

            raise PubNubException(
                pn_error=err,
                errormsg=text,
                status_code=res.status_code
            )

        return res

    def request_async(self, options, callback, cancellation_event):
        def success_callback(res):
            # http error
            status_category = PNStatusCategory.PNUnknownCategory
            response_info = None

            if res is not None:
                url = utils.urlparse(res.url)
                query = utils.parse_qs(url.query)
                uuid = None
                auth_key = None

                if 'uuid' in query and len(query['uuid']) > 0:
                    uuid = query['uuid'][0]

                if 'auth_key' in query and len(query['auth_key']) > 0:
                    auth_key = query['auth_key'][0]

                response_info = ResponseInfo(
                    status_code=res.status_code,
                    tls_enabled='https' == url.scheme,
                    origin=url.hostname,
                    uuid=uuid,
                    auth_key=auth_key,
                    client_request=res.request
                )

            if res.status_code != requests.codes.ok:
                if res.status_code == 403:
                    status_category = PNStatusCategory.PNAccessDeniedCategory

                if res.status_code == 400:
                    status_category = PNStatusCategory.PNBadRequestCategory

                if res.text is None:
                    text = "N/A"
                else:
                    text = res.text

                if res.status_code >= 500:
                    err = PNERR_SERVER_ERROR
                else:
                    err = PNERR_CLIENT_ERROR

                callback(status_category, res.json(), response_info, PubNubException(
                    pn_error=err,
                    errormsg=text,
                    status_code=res.status_code
                ))
            else:
                callback(status_category, res.json(), response_info, None)

        def error_callback(e):
            status_category = PNStatusCategory.PNBadRequestCategory
            # TODO: allow non PN errors

            if not type(e) is PubNubException:
                raise e

            if e._pn_error is PNERR_CONNECTION_ERROR:
                status_category = PNStatusCategory.PNUnexpectedDisconnectCategory
            elif e._pn_error is PNERR_CLIENT_TIMEOUT:
                status_category = PNStatusCategory.PNTimeoutCategory

            callback(status_category, None, None, e)

        client = AsyncHTTPClient(self, success_callback, error_callback, options, cancellation_event)

        thread = threading.Thread(target=client.run, name="PubNubThread")
        thread.start()

        return Call(thread, cancellation_event)

    # TODO: remove?
    def async_error_to_return(self, e, errback):
        errback(e)

    def stop(self):
        self._subscription_manager.stop()

    def request_deferred(self, options_func):
        raise PubNubException(pn_error=PNERR_DEFERRED_NOT_IMPLEMENTED)

    def add_listener(self, listener):
        assert isinstance(listener, SubscribeCallback)
        self._subscription_manager.add_listener(listener)
        # TODO: implement

# TODO: remove sync behaviour from twisted and tornado(move this to PubNub)
    def pn_request(self, session, scheme_and_host, headers, options, connect_timeout, read_timeout):
        assert isinstance(options, RequestOptions)
        url = scheme_and_host + options.path

        args = {
            "method": options.method_string,
            'headers': headers,
            "url": url,
            'params': options.query_string,
            'timeout': (connect_timeout, read_timeout)
        }

        if options.is_post():
            args['data'] = options.data
            logger.debug("%s %s %s %s" % (options.method_string, url, options.params, options.data))
        else:
            logger.debug("%s %s %s" % (options.method_string, url, options.params))

        # connection error
        try:
            res = session.request(**args)
        except requests.exceptions.ConnectionError as e:
            raise PubNubException(
                pn_error=PNERR_CONNECTION_ERROR,
                errormsg=str(e)
            )
        except requests.exceptions.HTTPError as e:
            raise PubNubException(
                pn_error=PNERR_HTTP_ERROR,
                errormsg=str(e)
            )
        except requests.exceptions.Timeout as e:
            raise PubNubException(
                pn_error=PNERR_CLIENT_TIMEOUT,
                errormsg=str(e)
            )
        except requests.exceptions.TooManyRedirects as e:
            raise PubNubException(
                pn_error=PNERR_TOO_MANY_REDIRECTS_ERROR,
                errormsg=str(e)
            )
        except Exception as e:
            raise PubNubException(
                pn_error=PNERR_UNKNOWN_ERROR,
                errormsg=str(e)
            )

        return res


class AsyncHTTPClient:
    """A wrapper for threaded calls"""

    def __init__(self, pubnub, success, error, options, cancellation_event, id=None):
        # TODO: introduce timeouts
        self.options = options
        self.success = success
        self.error = error
        self.pubnub = pubnub
        # seems useless
        self.cancellation_event = cancellation_event
        self.id = id

    def cancel(self):
        self.success = None
        self.error = None

    def run(self):
        try:
            res = self.pubnub.pn_request(
                 self.pubnub.session, self.pubnub.config.scheme_and_host(),
                 self.pubnub.headers, self.options,
                 self.pubnub.config.connect_timeout,
                 self.pubnub.config.non_subscribe_request_timeout)
            # print("Thread success", threading.current_thread())
            self.success(res)
        # except Exception as e:
        except PubNubException as e:
            # print("Thread error", str(e), threading.current_thread())
            self.error(e)


class Call(object):
    """
    A platform dependent representation of async PubNub method call
    """
    def __init__(self, thread, cancellation_event):
        self._thread = thread
        self._cancellation_event = cancellation_event
        self._is_executed = False
        self._is_canceled = False

    def is_executed(self):
        return self._is_executed

    def is_canceled(self):
        return self._is_canceled

    def cancel(self):
        # self._cancellation_event.set()
        self._thread._Thread_stop()
        self._is_canceled = True

    def join(self):
        if isinstance(self._thread, threading.Thread):
            self._thread.join()

    def executed(self):
        self._is_executed = True


class FakeThread(object):
    def join(self):
        pass
