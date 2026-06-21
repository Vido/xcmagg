from django.contrib.contenttypes.models import ContentType
from django.db.models import Q, Sum, Count, IntegerField, Avg, Subquery, OuterRef
from django.db.models import Exists

from nodes.models import NodeKind, Visibility
from engagement.models import Post, Vote, VoteMap, Rating


# A post is a "review" when its parent is a catalog/inventory/manufacturer node.
# Top-level community posts ("pocket dumps") have no such parent.
REVIEW_PARENT_KINDS = [
    NodeKind.CATALOG_ITEM,
    NodeKind.INVENTORY_ITEM,
    NodeKind.MANUFACTURER,
]


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
        # Top-level community posts only — exclude reviews (posts attached to a
        # catalog/inventory/manufacturer node), which have their own section.
        qs = (
            Post.visible.to(viewer, owner)
            .filter(
                node__kind=NodeKind.POST,
                node__owner=owner,
            )
            .exclude(node__parent__kind__in=REVIEW_PARENT_KINDS)
        )

        if order_by:
            qs = qs.order_by(order_by)

        return qs

    @staticmethod
    def visible_reviews_for(*, viewer, owner, order_by=None):
        # The owner's own reviews — posts attached to a catalog/inventory/
        # manufacturer node, scoped to the owner and the viewer's visibility.
        qs = (
            Post.visible.to(viewer, owner)
            .filter(
                node__kind=NodeKind.POST,
                node__owner=owner,
                node__parent__kind__in=REVIEW_PARENT_KINDS,
            )
            .select_related("node__parent")
        )

        if order_by:
            qs = qs.order_by(order_by)

        return qs


class RatingSelector:

    @staticmethod
    def aggregate_for(node):
        """Product score for a node. SINGLE weighting swap-point.

        v1 = raw average. To weight later (by review-post vote score or a future
        reputation), change only this method — `Rating.value` stays canonical.
        """
        agg = node.ratings.aggregate(avg=Avg("value"), count=Count("id"))
        return {"avg": agg["avg"], "count": agg["count"] or 0}

    @staticmethod
    def user_rating(node, user):
        """A user's rating value for a node, or None."""
        if not node or not getattr(user, "is_authenticated", False):
            return None
        r = node.ratings.filter(owner=user).first()
        return r.value if r else None

    @staticmethod
    def author_rating_subquery():
        """The post author's rating on the post's parent (catalog item)."""
        return Subquery(
            Rating.objects.filter(
                node_id=OuterRef("node__parent_id"),
                owner_id=OuterRef("node__owner_id"),
            ).values("value")[:1]
        )