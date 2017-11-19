from __future__ import unicode_literals

import cStringIO
import yaml

from mongoengine import StringField

from core.indicators import Indicator
from core.errors import IndicatorValidationError

# Taken from https://github.com/Neo23x0/sigma/wiki/Specification on 2017-11-19
template = """title
status [optional]
description [optional]
author [optional]
reference [optional]
logsource
   category [optional]
   product [optional]
   service [optional]
   definition [optional]
   ...
detection
   {search-identifier} [optional]
      {string-list} [optional]
      {field: value} [optional]
   ...
   timeframe [optional]
   condition
fields [optional]
falsepositives [optional]
level [optional]
...
[arbitrary custom fields]"""


class Sigma(Indicator):

    pattern = StringField(required=True, verbose_name="YAML", default=template)

    def clean(self):
        yaml_stream = cStringIO.StringIO(self.pattern)
        try:
            yaml.safe_load_all(self.pattern)
        except (yaml.parser.ParserError, yaml.scanner.ScannerError) as e:
            raise IndicatorValidationError("YAML parsing error: {}".format(e))

    def match(self, value):
        pass

# NOTE: Does it make sense to implement an "export" endpoint that would export
# a sigma rule to a given app? Given the possible configs this is probably not
# something we want to maintain.
# See: https://github.com/Neo23x0/sigma#supported-targets
