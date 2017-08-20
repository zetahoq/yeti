from __future__ import unicode_literals
from datetime import timedelta

from core.analytics import ScheduledAnalytics


class ExpireTags(ScheduledAnalytics):

    default_values = {
        "frequency": timedelta(hours=12),
        "name": "ExpireTags",
        "description": "Expires tags in observables",
    }

    ACTS_ON = []  # act on all observables

    # TODO Use server-side JS filter
    CUSTOM_FILTER = None

    EXPIRATION = timedelta(days=1)

    def bulk(self, observables):
        for o in observables:
            self.each(o)

    @staticmethod
    def each(obj):
        obj.expire_tags()
