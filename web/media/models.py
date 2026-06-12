from django.db import models
from django.utils import timezone


class Photo(models.Model):

    node = models.ForeignKey(
        'nodes.Node',
        on_delete=models.CASCADE,
        related_name="photos",
    )

    image = models.ImageField(upload_to="photos/")
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