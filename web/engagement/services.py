from django.db import transaction

from nodes.models import Node, NodeKind
from engagement.models import Post, Vote, VoteMap


class PostService:

    @staticmethod
    def create_post(*, owner, body, title, parent=None, photos=None):
        return PostService._materialize(
            kind=NodeKind.POST,
            owner=owner,
            parent=parent,
            body=body,
            title=title,
            photos=photos or [])

    @staticmethod
    def add_comment(*, owner, parent, body, photos=None):
        return PostService._materialize(
            kind=NodeKind.COMMENT,
            owner=owner,
            parent=parent,
            body=body,
            title=None,
            photos=photos or [])

    @staticmethod
    @transaction.atomic
    def _materialize(*, kind, owner, parent, body, title, photos):

        node = Node.objects.create(
            kind=kind,
            owner=owner,
            title=title,
            parent=parent,
        )

        post = Post.objects.select_for_update().get(node=node)
        post.body = body
        post.save(update_fields=["body"])

        for idx, image in enumerate(photos):
            Photo.objects.create(
                node=node,
                owner=owner,
                image=image,
                order=idx,
                is_primary=(idx == 0),
            )

        return post

    @staticmethod
    def edit(*, user, target, body):
        raise NotImplemented


class VoteService:
    @staticmethod
    @transaction.atomic
    def cast(*, user, node, action):
        Vote.objects.update_or_create(
            owner=user,
            node=node,
            defaults={"value": VoteMap.parse(action)},
        )