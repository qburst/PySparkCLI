from mongoengine import *
connect('mydb2')


class EtlData(Document):
    name = StringField(required=True, max_length=200)
    steps_to_desk = IntField()
    meta = {'allow_inheritance': True}
