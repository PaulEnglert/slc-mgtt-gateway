from datetime import datetime
import os
import sys
import argparse
import time
import json
import paho.mqtt.publish as MqttPublisher
from pycomm.ab_comm.slc import Driver as SlcDriver
from threading import Thread


class Logger():
    """A logger utility.
    """
    #           0       1        2       3          4
    _levels = ['TRACE', 'DEBUG', 'INFO', 'WARNING', 'ERROR']
    Level = 2

    @staticmethod
    def parse_level(stringLevel):
        stringLevel = stringLevel.lower()
        stringLevels = [l.lower() for l in Logger._levels]
        if stringLevel in stringLevels:
            return stringLevels.index(stringLevel)
        return None

    @staticmethod
    def _log(level, msg, fields=None):
        t = datetime.now()
        txt = '{} | {: ^7} | "{}"'.format(t.isoformat(), Logger._levels[level], msg)
        if fields is not None:
            txt += ' '
            for k, v in fields.items():
                txt += '{}={} '.format(k, v)
        if Logger.Level <= level:
            print txt

    @staticmethod
    def trace(msg, fields=None):
        Logger._log(0, msg, fields=fields)

    @staticmethod
    def debug(msg, fields=None):
        Logger._log(1, msg, fields=fields)

    @staticmethod
    def info(msg, fields=None):
        Logger._log(2, msg, fields=fields)

    @staticmethod
    def warning(msg, fields=None):
        Logger._log(3, msg, fields=fields)

    @staticmethod
    def error(msg, fields=None):
        Logger._log(4, msg, fields=fields)


class Message():
    """A message to use for publishing.

    Use this to create a message object
    that the broker can simply publish to
    the remote.

    A message is expected to contain a "data-update",
    meaning a timestamp/value tuple. Each message
    then belongs to a topic which identifies the
    series.

    Attributes
    ----------
    __uri : str
        The identifier for origin of this message
    __topic : str
        The mqtt topic to publish this at
    __timestamp : datetime
        The datetime of the message
    __value : any
        The value of the message
    """

    TOPIC_PREFIX = '/idatase'
    def __init__(self, uri, timestamp, value):
        """Create a new message.

        Parameters
        ----------
        uri : str
            The identifier for origin of this message
        timestamp : datetime
            The datetime of the message
        value : any
            The value of the message
        """
        self.__topic = '{}/{}'.format(Message.TOPIC_PREFIX, uri)
        self.__uri = uri
        self.__timestamp = timestamp
        self.__value = value
        Logger.trace('Created message', fields={
            'topic': self.__topic,
            'uri': self.__uri,
            'timestamp': self.__timestamp.isoformat(),
            'value': self.__value
        })

    def topic(self):
        """Return the topic of the message.
        """
        return self.__topic

    def payload(self):
        """Return the payload of the message.
        """
        return json.dumps({
            'topic': self.__topic,
            'uri': self.__uri,
            'timestamp': self.__timestamp.isoformat(),
            'value': self.__value
        })


class MqttBroker():
    """Publishing Broker to connect to a remote Broker.

    Use this to simply publish to a remote broker
    given host/port and authorization data.

    Attributes
    ----------
    host : str
        The host of the remote broker
    port : str
        The port of the remote broker
    username : str|None
        The username to use for authorization
    password : str|None
        The password to use for authorization
    """

    def __init__(self, host, port=1883, username=None, password=None):
        """Create a new Broker.

        Parameters
        ----------
        host : str
            The host of the remote broker
        port : str
            The port of the remote broker
        username : str|None
            The username to use for authorization
        password : str|None
            The password to use for authorization
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        Logger.trace('Created MqttBroker', fields={
            'host': host,
            'port': port,
            'username': username,
            'password': '***' if password is not None else None
        })

    def publish(self, message):
        """Publish a single message.

        This publishes the message to the remote broker.

        Parameters
        ----------
        message : Message
            The message to publish.
        """
        # ensure type
        assert isinstance(message, Message)
        self.publish_multiple([message])

    def publish_multiple(self, messages):
        """Publish multiple messages at once.

        This publishes the messages to the remote broker.

        Parameters
        ----------
        messages : Message[]
            The messages to publish.
        """
        # ensure type
        msgs = []
        for message in messages:
            assert isinstance(message, Message)
            msgs.append((message.topic(), message.payload()))
        # prep auth
        auth = None
        if self.username is not None:
            auth = {"username": self.username, "password": self.password}
        # publish messages
        Logger.trace('Publishing messages', fields={
            'count': len(msgs),
            'host': self.host,
            'port': self.port,
            'auth': True if auth is not None else False
        })
        MqttPublisher.multiple(
            msgs, hostname=self.host, port=self.port, auth=auth)


class SlcClient():
    """A simplified SlcClient.

    This client can be used to poll an SLC interface.

    Use add_subscription and remove_subscription to
    modify which address/tags are monitored and then
    run connect() to start polling, or disconnect()
    to stop again.

    
    See: https://pypi.org/project/pycomm/

    Attributes
    ----------
    _handlers : func[]
        The list of handlers to the subscribed tags.
    _poll_interval : float
        The polling interval to use for connecting to the PLCs.
    _endpoints : dict
        A dict of keys being addresses and the values being lists
        of tags to poll from the PLCs
    _run : bool
        Internal flag to signal stop to the polling thread
    """
    def __init__(self, poll_interval=5):
        """Create a new SlcClient.


        Parameters
        ----------
        poll_interval : float
            The itnerval in which to poll the PLCs
        """
        self._handlers = []
        self._poll_interval = poll_interval
        self._endpoints = {}
        self._run = False

        Logger.trace('Created SlcClient', fields={
            'poll_interval': poll_interval,
        })

    def add_subscription(self, address, tag):
        self._endpoints[address] = list(set(
            self._endpoints.get(address, []) + [tag]))
        Logger.info('Subscribed to PLC', fields={
            'address': address,
            'tag': tag,
        })

    def remove_subscription(self, address, tag):
        self._endpoints[address] = list(set(
            self._endpoints.get(address, [])).difference([tag]))
        Logger.info('Unsubscribed from PLC', fields={
            'address': address,
            'tag': tag,
        })

    def add_notification_handler(self, handler):
        """Add a handler function.

        This handler `def handler(update)` will be
        called upon reception of a notification from
        the SLC. The update will have the following data:
        ```{
            'address': <address>,
            'tag': <tag>,
            'value': <value>,
            'timestamp': <timestamp>
        }```
        """
        self._handlers.append(handler)

    def _publish(self, address, tag, timestamp, value):
        """Publish a data update.

        Parameters
        ----------
        address : str
            The IP address of the source PLC.
        tag : str
            The Tag name for which to publish.
        timestamp : datetime
            The timestamp of the scrape.
        value : any
            The value of the scrape.
        """
        data = {
            'address': address,
            'tag': tag,
            'timestamp': timestamp,
            'value': value
        }
        for hdl in self._handlers:
            hdl(data)

    def connect(self):
        """Connect the SLC Client.

        This starts the thread to poll the plcs
        data. Use disconnect() to stop again.
        """
        self._run = True
        thr = Thread(target=self._poll)
        thr.start()

    def _poll(self):
        """The blocking function pinging the plc for data.

        This scrapes all endpoints every n seconds and publishes
        the received data.
        """
        endpoints = []
        for a, v in self._endpoints.items():
            for t in v:
                endpoints.append((a, t))
        c = SlcDriver()
        Logger.info('Starting polling of PLCs', fields={
            'poll_interval': self._poll_interval,
        })
        while self._run:
            Logger.trace('starting polling iteration')
            for address, tag_name in endpoints:
                Logger.trace('reading', fields={
                    'address': address,
                    'tag_name': tag_name,
                })
                timestamp, value = None, None
                if c.open(address):
                    timestamp = datetime.now()
                    value = c.read_tag(tag_name)
                    Logger.trace('received value', fields={
                        'timestamp': timestamp.isoformat(),
                        'value': value,
                    })
                else:
                    Logger.warning('Failed connecting to PLC', fields={
                        'address': address,
                        'tag': tag_name
                    })
                c.close()
                if timestamp is not None and value is not None:
                    self._publish(address, tag_name, timestamp, value)
            time.sleep(self._poll_interval)

    def disconnect(self):
        """Signal the thread to stop again.
        """
        self._run = False


class Gateway():
    """SLC -> MQTT Gateway.
    """

    def __init__(self, slc_client, mqtt_broker):
        """Create a new SLC -> MQTT Gateway.
        """
        self.slc_client = slc_client
        self.mqtt_broker = mqtt_broker
        Logger.trace('Created Gateway', fields={
            'slc_client': slc_client,
            'mqtt_broker': mqtt_broker
        })
        # add notification handler
        def handler(update):
            try:
                self.route_update(update)
            except (Exception, IOError, RuntimeError) as e:
                Logger.error('Failed routing update: {}'.format(e))
        slc_client.add_notification_handler(handler)

    def route_update(self, update):
        """Route an update from the SLC Client to the MQTT Broker.

        Parameters
        ----------
        update : dict
            An update as received from the opc client.
        """
        name = '{}/{}'.format(update['address'], update['tag'])
        msg = Message(name, update['timestamp'], update['value'])
        Logger.trace('Routing update', fields={
            'id': name
        })
        self.mqtt_broker.publish(msg)

    def setup_proxy_for(self, subscriptions):
        """Add subscriptions at the SLC Client for the uris.

        Parameters
        ----------
        subscriptions : str
            A list of <IP-address>#tagname strings
        """
        for sub in subscriptions:
            splits = sub.split('/')
            address = splits.pop(0)
            tag = '/'.join(splits)
            self.slc_client.add_subscription(address, tag)

    def keep_alive(self):
        """
        """
        Logger.info('Locking Gateway')
        try:
            while True:
                time.sleep(1)
        except:
            Logger.info('Stopped Gateway')
            return


def run(
        mqtt_host, opc_path='/', mqtt_port=1883, mqtt_username=None,
        mqtt_password=None, mqtt_topic_prefix='/idatase', slc_poll_interval=5,
        subscriptions=[]):
    try:
        # Setup MQTT Broker
        Message.TOPIC_PREFIX = mqtt_topic_prefix
        mqtt_broker = MqttBroker(
            mqtt_host, port=mqtt_port, username=mqtt_username,
            password=mqtt_password)

        # Setup Opc Client
        slc_client = SlcClient(poll_interval=slc_poll_interval)

        # Define Gateway to Route Data
        gateway = Gateway(slc_client, mqtt_broker)

        # setup subscriptions to be proxied
        gateway.setup_proxy_for(subscriptions)

        # connect slc to PLCs
        slc_client.connect()

        # loop forever
        gateway.keep_alive()
    finally:
        slc_client.disconnect()


def parse_args():
    parser = argparse.ArgumentParser(
        description='SLC - MQTT Gateway.',
        epilog='''Any of the cli-arguments can be as well set through the 
        environment variables noted in the descriptions.
        The cli-arguments will always have priority over environment
        variables.''')

    # SLC Client Config
    parser.add_argument(
        '--slc-poll-interval', default=os.environ.get('GATEWAY_SLC_POLL_INTERVAL', 5),
        type=float, help='OPC Hostname. Env: GATEWAY_SLC_POLL_INTERVAL')

    # MQTT Broker
    parser.add_argument(
        '--mqtt-host', default=os.environ.get('GATEWAY_MQTT_HOST', 'localhost'),
        type=str, help='MQTT Hostname. Env: GATEWAY_MQTT_HOST')
    parser.add_argument(
        '--mqtt-port', default=os.environ.get('GATEWAY_MQTT_PORT', 1883),
        type=str, help='MQTT Port. Env: GATEWAY_MQTT_PORT')
    parser.add_argument(
        '--mqtt-username', default=os.environ.get('GATEWAY_MQTT_USERNAME'),
        type=str, help='MQTT Username. Env: GATEWAY_MQTT_USERNAME')
    parser.add_argument(
        '--mqtt-password', default=os.environ.get('GATEWAY_MQTT_PASSWORD'),
        type=str, help='MQTT Password. Env: GATEWAY_MQTT_PASSWORD')
    parser.add_argument(
        '--mqtt-topic-prefix', default=os.environ.get('GATEWAY_MQTT_TOPIC_PREFIX', '/idatase'),
        type=str, help='MQTT Topic Prefix. Env: GATEWAY_MQTT_TOPIC_PREFIX')

    # Subscription Config
    parser.add_argument(
        '-p', '--proxy-subscription', action='append', type=str,
        default=os.environ['GATEWAY_PROXY_SUBSCRIPTIONS'].split(';') if 'GATEWAY_PROXY_SUBSCRIPTIONS' in os.environ.keys() else [],
        help='Add a address/tag (e.g. "130.130.130.1/N54:60") combination to proxy from the SLC Client to the MQTT Broker. '
        'Env: GATEWAY_PROXY_SUBSCRIPTIONS (note the environment variable expects uris to be '
        'separated by a ";". Also note that whatever is set through the environment will be '
        'added to what is supplied through the cli options, unlike with the rest of the options, '
        'this is not overwritten)')

    # System Config
    parser.add_argument(
        '--log-level', type=str, default='INFO',
        help='Log Level: One of {}'.format(', '.join(Logger._levels)))

    return parser.parse_args()


if __name__ == '__main__':
    # parse args
    args = parse_args()

    # set log level
    if Logger.parse_level(args.log_level) is None:
        print 'Unknown loglevel: {}'.format(args.log_level)
        sys.exit(1)
    Logger.Level = Logger.parse_level(args.log_level)

    # run the gateway
    run(
        args.mqtt_host, mqtt_port=args.mqtt_port,
        mqtt_username=args.mqtt_username, mqtt_password=args.mqtt_password,
        mqtt_topic_prefix=args.mqtt_topic_prefix,
        slc_poll_interval=args.slc_poll_interval,
        subscriptions=list(set(args.proxy_subscription)))
