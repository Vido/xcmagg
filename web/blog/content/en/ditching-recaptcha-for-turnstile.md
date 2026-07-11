---
title: "Ditching reCAPTCHA for Turnstile"
description: "Why the CAPTCHA choice was never really about which puzzle is harder for bots — it's about who you're handing your visitors' data to."
lang: en
publish_date: 2026-07-06
updated_date: 2026-07-06
translation_group: ditching-recaptcha-for-turnstile
draft: true
tags: ["turnstile", "cloudflare", "privacy", "security", "indie-hacking"]
author: "Lucas Vido"
---

reCAPTCHA's real product was never "prove you're human." It's a tracking beacon that happens to also stop some bots, paid for in user friction — crosswalks, storefronts, traffic lights, however many rounds Google's model feels like asking for that day. Every page that embeds it hands Google a first-party read on a visitor who never chose to have a relationship with Google.

The decision to drop it wasn't a technical one. It was: who do I want my visitors' browsers talking to, and how much of a tax am I willing to charge them to prove they're not a bot.

## Privacy: one vendor relationship, not two

The site already sits behind Cloudflare — DNS, WAF, the free tier's edge network. Every request already carries `CF-Connecting-IP` and `CF-IPCountry` headers that other parts of the stack already trust (the geo-detection middleware reads them, so does the link-click tracker). Adding Turnstile means Cloudflare, a vendor already fully in the trust boundary, also handles bot-detection — instead of introducing Google as a *second* third party with its own cookie, its own cross-site profile-building, and its own privacy policy a visitor has to be comfortable with just to submit a form.

That's the actual argument. Not "Turnstile's algorithm is better." It's "I already trust this vendor with my traffic — I don't need a second one just for CAPTCHAs."

## Friction: invisible by default

reCAPTCHA v2 makes a visitor click a checkbox, and sometimes follows up with a puzzle. v3 is invisible but opaque — a score you don't see, tuned by a model you don't control, that occasionally decides a real visitor looks suspicious with no recourse. Turnstile's default mode runs a non-interactive challenge in the background and only escalates to something visible when it genuinely can't decide. Fewer real users get interrupted, and the ones who do get a widget, not a photo quiz.

## Easy to test — the point nobody mentions

This is the one that actually mattered day-to-day: Cloudflare publishes fixed dummy sitekeys and secrets that always pass, always fail, or always force the visible challenge — on purpose, for automated testing. CI can exercise every branch of the signup form (blocked, allowed, challenged) without mocking a third-party API or maintaining a "disable CAPTCHA in test mode" escape hatch that might silently leak into production. reCAPTCHA testing, by contrast, usually means either disabling it under `DEBUG` and hoping nobody forgets to re-enable it, or Google's own test keys, which come with more caveats. A CAPTCHA you can't reliably test is a CAPTCHA you'll eventually ship broken.

## The integration, briefly

Enough code to show the shape, not the whole implementation: it's a normal Django `forms.Field` wrapping a widget that renders Cloudflare's `<div data-sitekey>`, with a `clean()` that POSTs the token to Cloudflare's `siteverify` endpoint and fails closed on any network error. It participates in `form.is_valid()` like any other field — no separate "did you remember to check the CAPTCHA" step bolted onto the view.

## Honest limits

This doesn't eliminate trust, it relocates it. Cloudflare could have an outage, change pricing, or turn out to have its own data practices worth questioning someday. The argument isn't "Cloudflare is beyond reproach" — it's that the stack already depends on Cloudflare for DNS, edge, and the IP/geo headers three other components rely on. Adding a fourth thing to that same trust relationship is a smaller increase in surface area than adding a second vendor from scratch.

## What it unlocked

- One less third-party script and cookie on every page with a form
- A CAPTCHA that's actually testable in CI, with fixed keys built for exactly that
- Fewer real visitors interrupted by a puzzle
- One vendor relationship for bot-detection instead of two

The CAPTCHA question was never "which one's smarter." It was "whose beacon do you want running on your signup page."
