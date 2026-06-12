from urllib.parse import urlencode

from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_GET, require_POST
from django.contrib.auth import login    

from django.utils import timezone
from django_ratelimit.decorators import ratelimit

from magiclink.services import MagicLinkService
from magiclink.models import Event, Scope
from magiclink.services import TokenExpired, TokenInvalid, UserNotFound


@login_required
@require_POST
@ratelimit(key='user', rate='5/m', method='POST', block=True)
def generate_qr(request):
    """
    Desktop user generates QR code
    
    Rate limit: 5 requests per minute per user
    If exceeded, returns 429 Too Many Requests
    """
    next_url = request.POST.get('next', '/')
    
    token = MagicLinkService.create_token(
        user=request.user,
        scope=Scope.QRCODE,
        next_url=next_url,
        request=request
    )

    return render(request, 'magiclink/partials/qr_code.html', {
        'token': token,
        'magic_url': MagicLinkService.url(request, token),
        'created_at': timezone.now(),
        'ttl': 60,
    })


@require_GET
@ratelimit(key='ip', rate='5/m', method='GET', block=True)
def magic_login(request, token):
    try:
        user, next_url = MagicLinkService.redeem_token(
            token=token, request=request)
        login(request, user, backend='django.contrib.auth.backends.ModelBackend')
    except Exception as e:
        return render(request, 'magiclink/qr_invalid.html', {
            'error': str(e)
        })
    # MagicLinkService sanitizes next_url
    return redirect(next_url)


def ratelimit_handler(request, exception):
    return render(request, 'magiclink/rate_limit.html', status=429)