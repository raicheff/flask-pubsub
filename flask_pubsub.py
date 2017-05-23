#
# Flask-PubSub
#
# Copyright (C) 2017 Boris Raicheff
# All rights reserved
#


import base64
import json
import logging
import warnings

from flask import Blueprint, Response, abort, request
from flask.signals import Namespace
from six.moves.http_client import BAD_REQUEST, OK


logger = logging.getLogger('Flask-PubSub')


pubsub_message = Namespace().signal('pubsub.message')


class PubSub(object):
    """
    Flask-PubSub

    Documentation:
    https://flask-pubsub.readthedocs.io

    Google Cloud Pub/Sub Documentation:
    https://cloud.google.com/pubsub/docs

    :param app: Flask app to initialize with. Defaults to `None`
    """

    client = None

    topic = None

    verification_token = None

    codec = None

    def __init__(self, app=None, blueprint=None, client=None, codec=None):
        """"""

        if app is not None:
            self.init_app(app, blueprint, client, codec)

    def init_app(self, app, blueprint=None, client=None, codec=None):
        """"""

        blueprint = blueprint or Blueprint('pubsub', __name__)
        blueprint.add_url_rule('/pubsub', 'pubsub', self.handle_push, methods=('POST',))

        self.client = client

        self.topic = topic = app.config.get('PUBSUB_TOPIC')
        if topic is None:
            warnings.warn('PUBSUB_TOPIC not set', RuntimeWarning, stacklevel=2)

        self.verification_token = token = app.config.get('PUBSUB_VERIFICATION_TOKEN')
        if token is None:
            warnings.warn('PUBSUB_VERIFICATION_TOKEN not set', RuntimeWarning, stacklevel=2)

        self.codec = codec or json

    def publish(self, message, **kwargs):
        """"""

        self.client.topic(self.topic).publish(self.codec.dumps(message), **kwargs)

    def handle_push(self):
        """"""

        if request.args.get('token') != self.verification_token:
            abort(BAD_REQUEST)

        payload = json.loads(request.data.decode('utf-8'))
        logger.debug('payload=%s', payload)

        message = self.codec.loads(base64.b64decode(payload['message']['data']))
        logger.debug('message=%s', message)

        pubsub_message.send(self, message=message)

        return Response(status=OK)


# EOF
