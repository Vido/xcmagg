---
title: "Every Outbound Link Is a Redirect Now"
description: "Publishing a raw href is making a promise you might have to break later. Cloaking every outbound link fixes link rot, UTM discipline, and honest click data in one move."
lang: en
publish_date: 2026-07-06
updated_date: 2026-07-06
translation_group: every-outbound-link-is-a-redirect-now
draft: true
tags: ["linkcloak", "seo", "backlinks", "django", "indie-hacking"]
author: "Lucas Vido"
---

Publishing a raw `<a href="...">` to someone else's site is a bet that their URL never changes. It's a bad bet. Sites get redesigned, projects get renamed, domains lapse. Every post I've ever written with a direct outbound link is a post with a slowly decaying number of working links in it, and I have no way of knowing which ones without clicking every single one by hand.

## The problem, three ways

**Link rot.** A post from a year ago links straight to a project page that's since moved. Fixing it means finding every place I ever linked to it and editing each one.

**No signal.** I have no idea which outbound links anyone actually clicks. Not which post performs, not which recommendation lands — nothing.

**Manual UTM and `rel` discipline.** Every affiliate or sponsored link is supposed to carry `rel="sponsored"` (Google's own disclosure guidance) and, ideally, campaign parameters so I can tell where traffic came from. Typing that by hand, correctly, every single time, is a habit that will eventually lapse — and Google increasingly cares whether disclosed links are actually marked as such.

## The move: don't link out, link through

Every outbound link becomes `/go/<slug>/` instead of the destination directly. One `Link` row is the source of truth; the page just references the slug.

```python
class Link(models.Model):
    target_url = models.URLField(max_length=2000)
    slug = models.SlugField(max_length=64, unique=True, blank=True)
    cloak = models.BooleanField(default=True)
    rel_sponsored = models.BooleanField(default=False)
    rel_nofollow = models.BooleanField(default=True)
    rel_ugc = models.BooleanField(default=False)
```

Change the destination once, and every post that ever referenced that slug now points at the new place — no hunting through old content.

## `rel` attributes, computed, not remembered

```python
@property
def rel(self):
    tokens = ["noopener", "noreferrer"]
    if self.rel_sponsored: tokens.append("sponsored")
    if self.rel_nofollow: tokens.append("nofollow")
    if self.rel_ugc: tokens.append("ugc")
    return " ".join(tokens)
```

`noopener noreferrer` on every single outbound link, no exceptions, because that's not a judgment call — it's baseline hygiene against reverse tabnabbing. The disclosure tokens are a checkbox on the model instead of something I have to remember to type into markdown three months from now when I've forgotten which links were sponsored.

## UTM parameters that don't clobber the URL

```python
def get_target_url(self):
    utm = {k: v for k, v in {
        "utm_source": self.utm_source, "utm_medium": self.utm_medium,
        "utm_campaign": self.utm_campaign,
    }.items() if v}
    if not utm:
        return self.target_url
    parts = urlparse(self.target_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update(utm)
    return urlunparse(parts._replace(query=urlencode(query)))
```

Merges into whatever query string the destination already has instead of overwriting it — the kind of bug that's invisible until someone's existing tracking parameters silently disappear.

## Click data, minus the bots

```python
def is_bot(user_agent):
    if not user_agent or not user_agent.strip():
        return True
    return bool(BOT_UA_RE.search(user_agent))

def record_click(request, link):
    """Log a single click for a cloaked Link. Never raises into the redirect."""
    return ClickEvent.objects.create(
        link=link,
        ip_address=request.META.get("HTTP_CF_CONNECTING_IP"),
        user_agent=request.META.get("HTTP_USER_AGENT", ""),
        is_bot=is_bot(request.META.get("HTTP_USER_AGENT", "")),
    )
```

A click-log that counts every crawler and preview-fetcher hit is worse than no data at all — it's data you'll trust and shouldn't. The bot regex is deliberately dependency-free (crawlers, preview bots, and common HTTP libraries like `curl`/`python-requests`), and logging never raises into the redirect path — a broken analytics write should never be the reason a visitor's click fails.

## Honest limits

Cloaking your own links to your *own* other properties is a different animal from cloaking links to third parties — a closed loop of self-referential links, dressed up with tracking, can read to Google as a low-value link network rather than a genuine citation. This tool solves link rot and click visibility; it doesn't launder a backlink strategy that wasn't earning real third-party links in the first place.

## What it unlocked

- Change a destination once, every past post updates with it
- Real click data, bot traffic filtered out before it pollutes the count
- Correct `rel` disclosure on every sponsored/affiliate link, without relying on memory
- UTM tagging that's consistent because it's a form field, not a copy-paste habit

A link you control the redirect for is a link you can still be honest about a year later.
