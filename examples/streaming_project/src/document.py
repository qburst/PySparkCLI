from mongoengine import *
connect('streamdb')


class Tweet(Document):
    user = StringField(required=True, max_length=200)
    location = StringField(required=False, max_length=500)
    meta = {'allow_inheritance': True}
