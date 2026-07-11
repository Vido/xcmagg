import uuid as _uuid
from pathlib import Path

from django.db import models
from django.utils import timezone
from django.utils.text import slugify


def _photo_upload_path(instance, filename):
    ext = Path(filename).suffix[1:].lower() or 'jpg'
    slug = slugify(instance.node.title)[:50]
    shortcode = instance.node.shortcode
    suffix = _uuid.uuid4().hex[:6]
    return f"photos/{slug}-{shortcode}-{suffix}.{ext}"


class Photo(models.Model):

    node = models.ForeignKey(
        'nodes.Node',
        on_delete=models.CASCADE,
        related_name="photos",
    )

    image = models.ImageField(upload_to=_photo_upload_path)
    alt_text = models.CharField(max_length=255, blank=True)
    caption = models.CharField(max_length=500, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    is_primary = models.BooleanField(default=False)
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ["order", "created_at"]
        indexes = [
            models.Index(fields=["node"]),
            models.Index(fields=["node", "is_primary"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["node"],
                condition=models.Q(is_primary=True),
                name="one_primary_photo_per_node",
            ),
        ]

    def __str__(self):
        return self.image.name.rsplit("/", 1)[-1]