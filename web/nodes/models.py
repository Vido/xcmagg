from sqids import Sqids

from django.conf import settings
from django.db import models
from django.db.models.signals import post_save
from django.db.models import Q, Sum, Count
from django.dispatch import receiver

from django.utils import timezone
from django.utils.text import slugify
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _
from django.core.exceptions import ValidationError

_sqids = Sqids(min_length=4)


class NodeKind(models.TextChoices):
    POST = 'post', _('Post')
    CATALOG_ITEM = 'catalog_item', _('Catalog')
    INVENTORY_ITEM = 'inventory_item', _('Inventory')
    CATEGORY = 'category', _('Category')
    MANUFACTURER = 'manufacturer', _('Manufacturer')
    COMMENT = 'comment', _('Comment')
    USER_PROFILE = 'user_profile', _('UserProfile')

    @classmethod
    def get_model(cls, kind):
        """
        Returns the domain model class corresponding to a NodeKind.
        """
        from catalog import models as catalog_models
        from engagement import models as engagement_models
        from profiles import models as profile_models
        mapping = {
            cls.CATEGORY: catalog_models.Category,
            cls.MANUFACTURER: catalog_models.Manufacturer,
            cls.INVENTORY_ITEM: catalog_models.Item,
            cls.CATALOG_ITEM: catalog_models.Item,
            cls.POST: engagement_models.Post,
            cls.COMMENT: engagement_models.Post,
            cls.USER_PROFILE: profile_models.Profile,
        }
        try:
            return mapping[kind]
        except KeyError:
            raise AssertionError(f"Unhandled NodeKind: {kind}")

    @classmethod
    def get_domain_attr(cls, kind):
        mapping = {
            cls.CATEGORY: 'category',
            cls.MANUFACTURER: 'manufacturer',
            cls.INVENTORY_ITEM: 'item',
            cls.CATALOG_ITEM: 'item',
            cls.POST: 'post',
            cls.COMMENT: 'post',
            cls.USER_PROFILE: 'user_profile',
        }
        try:
            return mapping[kind]
        except KeyError:
            raise AssertionError(f"Unhandled NodeKind: {kind}")


class Visibility(models.TextChoices):
    PUBLIC = 'public', _('Public')
    UNLISTED = 'unlisted', _('Unlisted')
    PRIVATE = 'private', _('Private')
    #FOLLOWERS = "followers", _("Followers")


class Node(models.Model):

    # Future: a generic Tag(axis, value) model with an M2M here (related_name="tags")
    # would make any node faceted (discipline, tier, season); start with per-model
    # CharFields and migrate here once an axis needs multiple values per node.

    # Identity + ACL + routing

    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        db_index=True,
        related_name='nodes',
    )

    kind = models.CharField(
        max_length=32,
        choices=NodeKind.choices,
        db_index=True,
    )

    shortcode = models.CharField(
        max_length=16,
        unique=True,
        db_index=True,
        editable=False,
        null=True,
        blank=True,
    )

    parent = models.ForeignKey(
        'self',
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="children",
    )

    title = models.CharField(
        max_length=255,
        null=True,
        blank=True,
    )

    slug = models.SlugField(
        #unique=True,
        #editable=False,
        null=True,
        blank=True,
    )

    visibility = models.CharField(
        max_length=16,
        choices=Visibility.choices,
        default=Visibility.PUBLIC,
        db_index=True,
    )

    posts_allowed = models.BooleanField(default=True)
    votes_allowed = models.BooleanField(default=True)
    comments_allowed = models.BooleanField(default=True)

    #region = models.CharField(
    #    max_length=16,
    #    default="global",
    #    db_index=True,
    #)

    is_draft = models.BooleanField(default=False)
    published_at = models.DateTimeField(null=True, blank=True)

    is_deleted = models.BooleanField(default=False)

    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['kind', 'visibility']),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["owner", "kind"],
                condition=Q(kind=NodeKind.USER_PROFILE),
                name="unique_user_profile_node",
            )
        ]

    def __str__(self):
        parts = ['[DRAFT]'] if self.is_draft else []
        parts += [f'{self.get_kind_display()}: ']
        parts += [self.title] if self.title else []
        parts += [f'{self.shortcode}']
        return " ".join(parts)

    def save(self, *args, send_signals=True, **kwargs):

        if self.parent:
            if self.parent.kind in [NodeKind.COMMENT]:
                raise ValidationError("Comments cannot have a parent.")

        is_new = self.pk is None # Always before save()
        self.slug = slugify(self.title) if self.title else None
        self._send_signals = send_signals

        if not self.is_draft and self.published_at is None:
            self.published_at = timezone.now()

        super().save(*args, **kwargs)
        
        if is_new:
            self.shortcode = _sqids.encode([self.id])
            super().save(update_fields=['shortcode'])

    def get_absolute_url(self):
        domain_attr = NodeKind.get_domain_attr(self.kind)
        return getattr(self, domain_attr).get_absolute_url()

    @cached_property
    def comments(self):
        """
        Returns a queryset of all Comment objects that are attached
        directly or indirectly to this Node, including replies to replies.
        Uses a single query via the thread field.
        """
        return self.children.filter(
            kind=NodeKind.COMMENT,
            post__thread__startswith=str(self.id)
        ).select_related('owner', 'post').order_by('post__thread')

    @cached_property
    def _count(self):
        """Return a dict with up, down, and balance counts in a single query."""
        from engagement.models import VoteMap
        return self.votes.aggregate(
            total=Count('id'),
            upvotes=Count('id', filter=Q(value=VoteMap.UP)),
            downvotes=Count('id', filter=Q(value=VoteMap.DOWN))
        )

    upvotes = property(lambda self: self._count['upvotes'])
    downvotes = property(lambda self: self._count['downvotes'])
    score = property(lambda self: self.upvotes-self.downvotes)

    @property
    def balance(self):
        n = self._count['total']
        return 100 * self._count['upvotes'] / n if n else None

    def user_vote(self, user):
        if not user.is_authenticated:
            return None
        value = (
            self.votes.filter(owner=user)
            .values_list("value", flat=True).first()
        )
        from engagement.models import VoteMap
        return VoteMap(value) if value is not None else None

    @property
    def display_balance(self):
        return f'{round(self.balance)}%' if self.balance is not None else '?'

    @cached_property
    def primary_photo(self):
        return (
            self.photos.filter(is_primary=True).first() or 
            self.photos.all().first()
        )

    # Cache?
    def gallery(self, limit=5):
        return self.photos.order_by(
            '-is_primary', 'order', 'created_at')[:limit]


class VisibilityQuerySet(models.QuerySet):

    def public(self):
        return self.filter(
            node__visibility=Visibility.PUBLIC,
            node__is_draft=False,
        )

    def visible_to(self, viewer, owner=None):

        if viewer and owner and viewer == owner:
            return self

        qs = self.public()

        if owner:
            qs = qs.filter(node__owner=owner)

        # Placeholder for friend visibility
        # friends_q = Q(node__visibility=Visibility.FRIENDS) & Q(is_sold=False)
        # if viewer and are_friends(viewer, owner):
        #     public_q |= friends_q

        return qs


class NodeVisibilityManager(models.Manager):
    def get_queryset(self):
        return VisibilityQuerySet(self.model, using=self._db)

    def to(self, *args, **kwargs):
        return self.get_queryset().visible_to(*args, **kwargs)

    def public(self):
        return self.get_queryset().public()


class NodeBoundModel(models.Model):

    objects = models.Manager()
    visible = NodeVisibilityManager()

    class Meta:
        abstract = True

    @property
    def node(self):
        raise AttributeError("NodeBoundModel requires a `node` field")

    def __getattr__(self, name):
        try:
            node = object.__getattribute__(self, 'node')
        except Exception:
            raise AttributeError(name)

        try:
            return getattr(node, name)
        except AttributeError:
            raise AttributeError(name)

    def get_absolute_url(self):
        raise NotImplementedError

    def get_edit_url(self):
        raise NotImplementedError

    """
    #_node_field_names_cache = None  # class-level cache for all subclasses
    def __getattribute__(self, name):
        # always try Node delegation first
        node = super().__getattribute__('node')
        cls = type(self)

        # cache Node fields per class
        if cls._node_field_names_cache is None:
            cls._node_field_names_cache = [
                f.name for f in type(node)._meta.get_fields() if isinstance(f, models.Field)
            ]

        if name in cls._node_field_names_cache:
            return getattr(node, name)

        # fallback to normal attribute access
        return super().__getattribute__(name)
    """


@receiver(post_save, sender=Node)
def create_domain_object(sender, instance, created, **kwargs):

    if not created:
        return

    if not getattr(instance, "_send_signals", True):
        return

    DomainModel = NodeKind.get_model(instance.kind)
    from profiles.models import Profile
    if DomainModel is Profile:
        # allauth and loops problems
        return

    DomainModel.objects.create(node=instance)