from django.contrib.auth import login
from django.contrib.auth.decorators import login_required
from django.http import HttpResponseBadRequest
from django.shortcuts import redirect, render
from django.utils.timezone import now
from django.db import transaction
from django.shortcuts import render
from django.contrib import messages
from django.views.decorators.http import (
    require_POST,
    require_http_methods
)
from django.utils.http import url_has_allowed_host_and_scheme
from django.utils.translation import gettext_lazy as _
from django.contrib.auth import get_user_model

from django_ratelimit.decorators import ratelimit
from allauth.account.models import EmailAddress

from magiclink.services import (
    MagicEmailService, MagicLinkService
)
from profiles.models import Profile
from profiles.forms import CustomSignupForm
from profiles.services import YeetService
from profiles.turnstile import TurnstileValidator


User = get_user_model()

@require_POST
@ratelimit(key='post:email', rate='3/m', method='POST', block=True)
@ratelimit(key='ip', rate='5/m', method='POST', block=True)
def identity_gate(request):

    next_url = request.POST.get("next", "/")
    if not url_has_allowed_host_and_scheme(
        next_url, allowed_hosts={request.get_host()}):
        return HttpResponseBadRequest("Something fishy happend...")

    email = request.POST.get("email", "").lower().strip()
    template = "profiles/partials/identity_gate.html"

    if not email:
        context = {
            "form_error": _("Please enter a valid email address."),
            "yeet": False
        }
        return render(request, template, context)

    if not TurnstileValidator.verify(request):
        context = {
            "form_error": _("Captcha verification failed. Please try again."),
            "yeet": False
        }
        return render(request, template, context)

    email_address = (
        EmailAddress.objects
        .filter(
            email=email,
            verified=True,
            primary=True)
        .select_related("user")
        .first()
    )

    # --- Existing user → login ---
    if email_address and email_address.user:
        YeetService.authenticate(
            user=email_address.user,
            request=request,
            next_url=next_url
        )
        messages.success(request, 'We sent you a secure login link.')
        context = {
            "mode": "login",
            "email": email,
            "yeet": True
        }

    # --- Claimed email or new email → onboarding ---
    else:
        YeetService.verify(
            email=email,
            request=request,
            next_url=next_url
        )
        messages.success(request, 'We sent you a secure sign-up link. Check your email.')
        context = {
            "mode": "signup",
            "email": email,
            "yeet": True
        }

    return render(request, template, context)

@require_http_methods({"GET", "POST"})
@ratelimit(key='ip', rate='5/m', block=True)
def onboarding(request, token):

    try:
        new_token, data = MagicEmailService.rotate(token)
    except Exception as e:
        return render(request, 'magiclink/qr_invalid.html', {
            # Popular Gear
            # Recent Posts
            'error': str(e)
        })    

    email = data['email']
    magic_url = MagicEmailService.url(request, new_token)

    if request.method == 'GET':
        messages.success(request, 'Your email has been verified.')
        return render(request, 'profiles/onboarding.html', {
            'email': email,
            'magic_url': magic_url,
            'form': CustomSignupForm(email=email, request=request),
        })

    # POST: Process signup
    form = CustomSignupForm(request.POST, email=email, request=request)
    if not form.is_valid():
        messages.error(request, f'{form.errors}')
        return render(request, 'profiles/onboarding.html', {
            'form': form,
            'magic_url': magic_url,
        })

    with transaction.atomic():
        user = form.save(request)
        email, next_url = MagicEmailService.redeem_claim(
            token=new_token, request=request)

    login(request, user, backend='django.contrib.auth.backends.ModelBackend')
    messages.success(request, 'Welcome! Your account has been created.')

    # MagicEmailService sanitizes next
    return redirect(next_url)