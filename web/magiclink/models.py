from django.db import models
from django.contrib.auth import get_user_model
from django.utils.translation import gettext_lazy as _

User = get_user_model()


class Event(models.TextChoices):
    CREATED = 'c', _('Created')
    SUCCESS = 's', _('Success')
    FAILURE = 'f', _('Failure')
    EXPIRED = 'e', _('Expired')

    
class Scope(models.TextChoices):
    EMAIL = 'e', _('Email')
    QRCODE = 'q', _('QR Code')


class AuditEvent(models.Model):    
    email = models.EmailField(db_index=True, null=True, blank=True)
    event_type = models.CharField(max_length=1, choices=Event, db_index=True)
    scope = models.CharField(max_length=1, choices=Scope, null=True, blank=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    user_agent = models.TextField(null=True, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True, db_index=True)
    
    class Meta:
        indexes = [
            models.Index(fields=['email', '-timestamp']),
            models.Index(fields=['event_type', '-timestamp']),
        ]
