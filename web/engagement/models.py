from django.db import models
from django.db.models import Count, Q
from django.urls import reverse
from django.conf import settings
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _

from nodes.models import NodeKind, NodeBoundModel


class VoteMap(models.IntegerChoices):
    UP = 1, "Up"
    DOWN = -1, "Down"

    @classmethod
    def parse(cls, action):
        try:
            return cls[action.upper()]
        except KeyError:
            raise ValueError(f"Invalid vote action: {action}")


class NodeReaction(models.Model):
    """Abstract base for per-(node, owner) signals attached to a Node.

    Concrete reactions (Vote, Rating) share the same shape and one-per-user
    constraint, but keep their own `value` semantics and isolated aggregates.
    """

    node = models.ForeignKey(
        'nodes.Node',
        on_delete=models.CASCADE,
        related_name="%(class)ss",
    )

    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="%(class)ss",
    )

    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        abstract = True
        unique_together = ("node", "owner")
        indexes = [
            models.Index(fields=["node"]),
        ]


class Vote(NodeReaction):
    value = models.SmallIntegerField(choices=VoteMap.choices)


class Rating(NodeReaction):
    MIN, MAX = 1, 5
    value = models.PositiveSmallIntegerField(
        choices=[(i, str(i)) for i in range(MIN, MAX + 1)]
    )


class Post(NodeBoundModel, models.Model):

    node = models.OneToOneField(
        'nodes.Node',
        on_delete=models.CASCADE,
        related_name="post",
        limit_choices_to={"kind__in": 
            [NodeKind.POST, NodeKind.COMMENT]
        }
    )

    depth = models.PositiveSmallIntegerField(default=0, editable=False)
    thread = models.CharField(max_length=64, default='', editable=False)

    reply_to = models.ForeignKey(
        "self",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="replies",
    )

    body = models.TextField()

    class Meta:
        indexes = [
            models.Index(fields=["node"]),
        ]

    @property
    def display_name(self):
        return self.title or "Untitled"

    def __str__(self):
        snippet = (self.body[:50] + "...") if len(self.body) > 50 else self.body
        return f"{self.node.owner.username}: {snippet}"

    def save(self, *args, **kwargs):
        """
        Build the thread and depth before saving:
        - Top-level comments: thread starts from node.parent (the Node being commented on)
        - Replies: thread extends reply_to.thread
        """
        # Ensure the node exists first to get node_id
        if not self.node_id:
            super().save(*args, **kwargs)

        if self.reply_to:
            # Replying to another comment
            self.depth = getattr(self.reply_to, 'depth', 0) + 1
            self.thread = f"{self.reply_to.thread}.{self.node_id}"
        else:
            # Top-level comment, thread root = node.parent
            parent_node_id = getattr(self.node.parent, 'id', self.node_id)
            self.depth = 0
            self.thread = str(parent_node_id)

        super().save(*args, **kwargs)

    is_post = property(lambda self: self.kind == NodeKind.POST)
    is_comment = property(lambda self: self.kind == NodeKind.COMMENT)

    def get_absolute_url(self):
        return reverse(
            'post-details', kwargs={
                'shortcode': self.shortcode, 'slug': self.slug
            },
        )

    def get_edit_url(self):
        return reverse('edit-post',
            kwargs={"shortcode": self.shortcode}
        )

    def owner_absolute_url(self):
        return self.owner.profile.get_absolute_url()

"""
class WishlistItem(models.Model):

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="wishlist_items",
    )

    item = models.ForeignKey(
        "catalog.Item",
        on_delete=models.CASCADE,
    )

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("user", "item")
"""