from django.db import models
from django.conf import settings
from django.urls import reverse
from nodes.models import NodeKind, NodeBoundModel


class Profile(NodeBoundModel, models.Model):

    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="profile",
    )

    node = models.OneToOneField(
        'nodes.Node',
        on_delete=models.CASCADE,
        related_name="user_profile",
        limit_choices_to={"kind": NodeKind.USER_PROFILE},
    )

    version_tos = models.CharField(max_length=20, default='v0')
    last_tos = models.DateTimeField(blank=True, null=True)

    last_verification = models.DateTimeField(blank=True, null=True, db_index=True)

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        self.sync_profile_node()

    def get_absolute_url(self):
        return reverse('user-inventory',
            kwargs={'username': self.user.username}
        )

    def get_edit_url(self):
        raise NotImplementedError

    def sync_profile_node(self):
        if self.title != self.user.username:
            self.node.title = self.user.username
            self.node.save(update_fields=["title"])

    def is_complete(self) -> bool:
        # TODO
        return bool(self.last_tos)



# class Preferences(models.Model):
#     # TODO
