# web (edcx) — TODO

Architectural backlog. Issues are coupled; build in the order below.

Full plan (context, current-state file:line citations, per-issue approach, verification):
[`plans/web-edcx-issues-r2-thumbor-multilang-affiliate.md`](../plans/web-edcx-issues-r2-thumbor-multilang-affiliate.md)

Locked decisions: R2 storage + self-hosted **thumbor**; 3-layer multilang (market-segmented content + field/UGC translation + UI); **GeoIP2** (MaxMind GeoLite2) for geo.

## Bugs

- [ ] **Welcome flow broken — no email provider configured.** Sign-up welcome email fails; will use Postmark (per prod email backend). Configure Postmark token/sender in prod `web/.env` + verify welcome email send path.
- [ ] **Prod login fails — Turnstile siteverify 400.** Cloudflare returns `400 Bad Request` from `/turnstile/v0/siteverify` → `validate_turnstile` (`web/profiles/turnstile.py:23`) catches → `TurnstileField.clean` raises → form re-renders (POST 200, no redirect). Cause: malformed/empty `TURNSTILE_SECRET_KEY` in prod `web/.env` (a bad-but-present secret gives 200 + `invalid-input-secret`; raw 400 = empty/whitespace/stray quotes). Fix: correct secret in server `web/.env`, restart web container.

## Build order

- [ ] **1. Image storage → R2** _(foundation)_
  - `django-storages[s3]` + `boto3`, R2 via Django `STORAGES`. R2 only in `prod_settings` (replace `# TODO: R2 STORAGE`, `prod_settings.py:45`); dev keeps FileSystemStorage.
  - Dev/prod parity + backups: separate staging/prod buckets, R2 versioning + scheduled copy.
  - One-off mgmt command to migrate existing `/data/uploads/photos/` → R2.

- [ ] **2. Crop / thumbnails → thumbor** _(reads R2 origin, depends on #1)_
  - thumbor container behind `lvido-proxy` (named docker network per CLAUDE.md), loader reads R2.
  - `web/media/templatetags/thumbs.py` → signed thumbor URLs; replace raw `{{ primary.image.url }}` (`_item_card.html:11`, `_post_card.html:13`, admin `primary_thumb`).
  - Named sizes: card / thumb / detail / og.

- [ ] **3. Multilang** _(establishes Market + GeoIP2, before #4)_
  - [ ] 3a. UI: `LocaleMiddleware`, `LANGUAGES`, `LOCALE_PATHS`, `{% trans %}`, fix hardcoded `lang="en"` (`base.html:1`).
  - [ ] 3b. Field/UGC: `django-modeltranslation` on `Item.description`, Category/Manufacturer names. UGC author-language field (machine-translate later).
  - [ ] 3c. Market-segmented: `Market` model (ISO country→market) + GeoIP2 resolution middleware (cookie/session override), market-aware querysets. **Shared geo primitive reused by #4.**
  - [ ] 3d. Sitemap hreflang: add per-language `<xhtml:link rel="alternate">` entries to catalog/category/manufacturer sitemaps (`config/sitemaps.py`) once languages are defined.

- [ ] **4. Geo-targeted affiliate links** _(reuses Market + GeoIP2, depends on #3)_
  - `market` FK/M2M on `RetailerLink` (`web/catalog/models.py:165`); null = global fallback.
  - `RetailerLink.for_market(market)` manager; prefer market link, fall back global.
  - Update `_retailer_links.html` (`item_detail.html:186`) + form/admin to pass filtered qs + expose market field.

- [ ] **5. Share buttons + og:cards**
  - Per-page Open Graph + Twitter Card meta (title, description, `og:image`, `og:type`, canonical URL).
  - `og:image` from item primary photo (thumbor `og` size, depends on #2); fallback default card image.
  - Share buttons on item detail / list pages (native Web Share API + WhatsApp/X/Facebook/copy-link fallbacks).
  - Validate w/ FB Sharing Debugger + X Card Validator.
