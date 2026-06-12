import requests
from collections import UserDict

from django.conf import settings
from django.core.exceptions import ValidationError
from django.utils.safestring import mark_safe
from django import forms

def validate_turnstile(token, remoteip=None):
    """ from: https://developers.cloudflare.com/turnstile/get-started/server-side-validation/ """
    data = {'secret': settings.TURNSTILE_SECRET, 'response': token}
    if remoteip:
        data['remoteip'] = remoteip

    try:
        response = requests.post(
            'https://challenges.cloudflare.com/turnstile/v0/siteverify',
            data=data, timeout=10)
        response.raise_for_status()
        obj = response.json()

    except requests.RequestException as e:
        print(f"Turnstile validation error: {e}")
        obj = {'success': False, 'error-codes': ['internal-error']}

    return obj


class TurnstileValidator():

    @staticmethod
    def verify(request):
        """ Syntax sugar """
        class Compat(UserDict):
            def __bool__(self):
                return bool(self.data.get('success', False))

        return Compat(validate_turnstile(
            token=request.POST.get('cf-turnstile-response'),
            remoteip=request.META.get('REMOTE_ADDR'),
        ))


class TurnstileWidget(forms.Widget):
    template_name = None

    def __init__(self, size='normal', theme='auto', attrs=None):
        super().__init__(attrs)
        self.size = size if size in ('normal', 'flexible', 'compact') else 'normal'
        self.theme = theme if theme in ('auto', 'light', 'dark') else 'auto'
        self.data_size = {
            'normal': '',
            'flexible': 'data-size="flexible"',
            'compact': 'data-size="compact"',
        }[self.size]

    def render(self, name, value, attrs=None, renderer=None):
        html = f"""
        <div class="cf-turnstile"
             data-sitekey="{settings.TURNSTILE_SITE_KEY}"
             {self.data_size}
             data-theme="{self.theme}">
        </div>
        """
        return mark_safe(html)


class TurnstileField(forms.Field):
    widget = TurnstileWidget

    def clean(self, value):
        if not hasattr(self, "request") or not self.request:
            raise ValidationError("Request not found")
        obj = TurnstileValidator.verify(self.request)
        if not obj:
            errors = obj.get("error-codes")
            if errors:
                raise ValidationError(", ".join(errors))
            raise ValidationError("Captcha validation failed")

        return obj


