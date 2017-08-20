from __future__ import unicode_literals

from core.database.fields import ListField, StringField, StringListField

from core.entities import Entity
from core.database.fields import TagListField, StringListField


class Actor(Entity):

    aliases = ListField(StringField(), verbose_name="Aliases")

    DISPLAY_FIELDS = Entity.DISPLAY_FIELDS + [("aliases", "Aliases")]

    @classmethod
    def get_form(klass):
        form = Entity.get_form(override=klass)
        form.aliases = StringListField("Aliases")
        return form

    def generate_tags(self):
        return [self.name.lower()]

    def info(self):
        i = Entity.info(self)
        i['aliases'] = self.aliases
        i['type'] = "Actor"
        return i
