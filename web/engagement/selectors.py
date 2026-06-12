from django.contrib.contenttypes.models import ContentType
from django.db.models import Q, Sum, Count, IntegerField
from django.db.models import Exists, OuterRef

from nodes.models import NodeKind, Visibility
from engagement.models import Post, Vote, VoteMap


class PostSelector:

    @staticmethod
    def base_qs():
        return (
            Post.visible.public()
            .filter(node__kind=NodeKind.POST) # filters out comments
            .select_related("node", "node__owner")
        )

    @staticmethod
    def posts_feed(limit=20):
        comment_count = (
            PostSelector.base_qs()
            .filter(
                thread__startswith=OuterRef("thread"),
                node__kind=NodeKind.COMMENT,
            )
            .values("thread")
            .annotate(c=Count("id"))
            .values("c")[:1]
        )

        return (
            PostSelector.base_qs()
            .annotate(
                score=Coalesce(Sum("node__votes__value"), Value(0)),
                comment_count=Coalesce(
                    Subquery(comment_count, output_field=IntegerField()),
                    Value(0),
                ),
            )
            .order_by("-node__created_at")[:limit]
        )

    @staticmethod
    def highlighted(limit=6):
        from media.models import Photo

        photos = Photo.objects.filter(node=OuterRef("node_id"))
        likes_count = Count("node__votes",
            filter=Q(node__votes__value=VoteMap.UP),
            distinct=True,
        )
        comments_count = Count(
            "node__children",
            filter=Q(node__children__kind=NodeKind.COMMENT),
            distinct=True,
        )
        return (
            PostSelector.base_qs()
            .annotate(
                has_photo=Exists(photos),
                likes_count=likes_count,
                comments_count=comments_count,
            )
            .filter(has_photo=True)
            .order_by("-node__published_at")[:limit]
        )

    @staticmethod
    def posts_under(node):
        return (
            PostSelector.base_qs()
            .filter(node__parent=node)
        )

    @staticmethod
    def posts_under_parent(node_kinds: list):
        return (
            PostSelector.base_qs()
            .select_related("node__parent")
            .filter(
                node__parent__kind__in=node_kinds,
                node__kind=NodeKind.POST,
            )
        )

    @staticmethod
    def visible_posts_for(*, viewer, owner, order_by=None):
        qs = (
            Post.visible.to(viewer, owner)
            .filter(
                node__kind=NodeKind.POST,
                node__owner=owner
            )
        )

        if order_by:
            qs = qs.order_by(order_by)

        return qs