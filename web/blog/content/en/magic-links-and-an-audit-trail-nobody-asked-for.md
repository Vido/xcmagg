---
title: "Magic Links, QR Logins, and an Audit Trail Nobody Asked For"
description: "Passwordless login done properly: single-use tokens, rate limiting, and a full audit trail — built on allauth, not around it."
lang: en
publish_date: 2026-07-06
updated_date: 2026-07-06
translation_group: magic-links-and-an-audit-trail-nobody-asked-for
draft: true
tags: ["magiclink", "django", "auth", "security", "indie-hacking"]
author: "Lucas Vido"
---

The best password is the one nobody has to remember, type, or leak.

That's not a slogan — it's the whole argument for magic links. Every password you make a user set is a password they'll reuse, forget, or type into a phishing page that looks close enough. You don't need to win that fight. You just need to not have it.

## The shape of the problem

I wanted two things that are usually sold as separate products: a login flow you can do without typing anything, and a way to hand off a session from one device to another — sit at a desktop, scan a code with your phone, you're in. Both are "prove you own this identity," just entered from different doors.

And I wanted neither of them to leave a password sitting in a database waiting to be the reason for an incident report.

## Tokens that don't live in a table

The obvious move is a `Token` model: a row per link, an expiry column, a `used` boolean you forget to check somewhere. Instead, the token *is* the cache key.

```python
@classmethod
def _set(klass, data: Dict, timeout: int) -> str:
    token = secrets.token_urlsafe(32)
    cache.set(f"{klass.PREFIX}:{token}", data, timeout=timeout)
    return token

@classmethod
def _burn(klass, token: str) -> Dict:
    if not token:
        raise TokenInvalid("Token is required")
    key = f'{klass.PREFIX}:{token}'
    data = cache.get(key)
    cache.delete(key)  # Consume token (single-use)
    if not data:
        raise TokenExpired("Token has expired or been consumed")
    return data
```

No migration, no cleanup cron for expired rows, no race where two requests both see `used=False`. `cache.get` + `cache.delete` isn't atomic against a determined attacker hammering the same token in parallel, but for the threat model here — a link sitting in someone's inbox — it's exactly enough machinery and not a gram more.

Two prefixes, two lifetimes: `magic_link` tokens carry a `user_id` and live 60 seconds (this is a live handoff — you're scanning a QR code *right now*), `email_claim` tokens carry a bare `email` and live 300 seconds (this is "check your inbox," people are slower than a phone camera).

## Two flows, one mechanism

**QR login.** You're logged in on desktop, you want the same session on your phone without touching a keyboard.

```python
@login_required
@require_POST
@ratelimit(key='user', rate='5/m', method='POST', block=True)
def generate_qr(request):
    token = MagicLinkService.create_token(
        user=request.user, scope=Scope.QRCODE,
        next_url=request.POST.get('next', '/'), request=request
    )
    return render(request, 'magiclink/partials/qr_code.html', {...})
```

Scan it, and:

```python
@require_GET
@ratelimit(key='ip', rate='5/m', method='GET', block=True)
def magic_login(request, token):
    user, next_url = MagicLinkService.redeem_token(token=token, request=request)
    login(request, user, backend='django.contrib.auth.backends.ModelBackend')
    return redirect(next_url)
```

**Email claim.** Different problem: you don't have a user yet, you just want to prove someone owns an inbox before you commit to creating an account for it.

```python
if EmailAddress.objects.filter(email=email, verified=True).exists():
    raise TokenInvalid("This email already exists")

token = MagicEmailService._set(
    {"ttl": ttl, "email": email, 'scope': Scope.EMAIL, 'next_url': next_url},
    timeout=ttl,
)
```

No `User` row created. Just a claim: *this email, right now, wants in*. That's the primitive underneath anything that says "enter your email to continue" — an onboarding flow today, a gated ebook download later.

## Rate limits and the paper trail

Both endpoints are rate-limited (`5/m`, keyed on user for the authenticated QR generator, on IP for the anonymous redeem), and every single event — created, success, failure, expired — writes an `AuditEvent`: email, scope, IP (via Cloudflare's `CF-Connecting-IP`), user agent, timestamp.

```python
except TokenError as e:
    AuditEvent.objects.create(
        event_type=Event.FAILURE, ip_address=ip_address, user_agent=user_agent,
    )
    raise
```

Nobody asked for this table. I built it anyway, because "someone is hammering `/magic-login/<token>/`" is a question you want to be able to answer in one query, not by grepping nginx logs at 2am.

`next_url` gets sanitized through `url_has_allowed_host_and_scheme` before it's ever trusted — the one-line mistake that turns a login flow into an open redirect for phishing.

## When would you actually need more?

This isn't SSO. There's no SAML, no cross-domain session sharing, no support for "log in with your work identity provider." If you need that, you need `django-allauth`'s SSO providers or a real identity broker, not a home-rolled token cache. This is the right tool for "one app, first-party accounts, low-friction entry" — not for enterprise federation.

## What it unlocked

- Login with zero passwords, zero reset-flow support tickets
- Cross-device handoff (desktop → phone) in one scan
- An audited, rate-limited paper trail for every auth event, not just the successful ones
- A verified-email primitive that doesn't force a signup — the exact shape "email-gate this ebook" will need later

Passwords are a liability you accept by default. Turns out you don't have to.
