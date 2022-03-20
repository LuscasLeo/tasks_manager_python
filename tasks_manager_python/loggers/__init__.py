

from logging import StreamHandler
from sys import stdin


class BasicMessageConsumerLogHandler(StreamHandler):
    def __init__(self, stream=stdin):
        super().__init__(stream)