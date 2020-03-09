from mongoengine import *
connect('mydb')


class BlogPost(Document):
    title = StringField(required=True, max_length=200)
    tags = ListField(StringField(max_length=50))
    meta = {'allow_inheritance': True}


class TextPost(BlogPost):
    content = StringField(required=True)


class LinkPost(BlogPost):
    url = StringField(required=True)


post1 = TextPost(title='Using MongoEngine', content='See the tutorial')
post1.tags = ['mongodb', 'mongoengine']
post1.save()

post2 = LinkPost(title="New Link Post", url="linkpost.com")
post2.tags = ['test1', 'test2']
post2.save()