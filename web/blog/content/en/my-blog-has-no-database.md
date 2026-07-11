---
title: "My Blog Has No Database"
description: "Markdown files, frontmatter, and Django's file cache keyed by mtime — the entire blog engine, no models, no migrations."
lang: en
publish_date: 2026-07-06
updated_date: 2026-07-06
translation_group: my-blog-has-no-database
draft: true
tags: ["blog", "django", "markdown", "python", "indie-hacking"]
author: "Lucas Vido"
---

Every blog tutorial starts the same way: `class Post(models.Model)`. Title, slug, body, a foreign key to an author, `makemigrations`, `migrate`, an admin registration so you can type your posts into a textarea in the Django admin instead of a text editor. For a blog with exactly one author who already has a text editor, that's a lot of ceremony to reinvent Notepad.

## The source of truth is a file

There's no `Post` model here. A blog post is a file: `blog/content/<lang>/<slug>.md`, frontmatter on top, markdown underneath.

```python
CONTENT_DIR = Path(settings.BASE_DIR) / "blog" / "content"
LANGS = ("en", "pt-br")
DEFAULT_LANG = "en"
```

Git already gives me versioning, diffs, and a review flow (a PR *is* the editorial workflow) for free. Why put that behind a database row and then rebuild git-with-extra-steps on top of it?

## Caching without an invalidation strategy

The one real objection to file-backed content is performance — parsing markdown on every request is wasteful. The usual fix is a cache with an expiry and a signal handler to bust it on save. I didn't want to write that handler, so I made the cache key describe the file instead of trusting a TTL:

```python
def get_post(lang: str, slug: str) -> tuple[PostMeta, str]:
    path = _path(lang, slug)
    key = f"blog:post:{lang}:{slug}:{path.stat().st_mtime_ns}"
    cached = _cache().get(key)
    if cached is not None:
        return cached
    ...
```

Edit the file, its mtime changes, the key changes, the old entry is simply never looked up again — no expiry needed, no signal to wire up, no stale-cache bug class to worry about. The list view does the same trick at directory granularity:

```python
def _dir_signature(folder: Path) -> str:
    """Hash of (name, mtime) for every post — changes on add/edit/delete."""
    parts = [f"{p.name}:{p.stat().st_mtime_ns}" for p in sorted(folder.glob("*.md"))]
    return hashlib.md5("|".join(parts).encode()).hexdigest()
```

Add, edit, or delete any post and the archive listing's cache key changes with it. It's on Django's file-based cache backend too, deliberately, so rendered HTML sits on disk instead of competing with real data in Redis/RAM.

## i18n is one field

Every post has a `translation_group` — an arbitrary string shared by every language version of the same idea:

```python
def siblings(translation_group: str) -> dict[str, str]:
    """{lang: slug} for every language that has this translation group."""
    out: dict[str, str] = {}
    for lang in LANGS:
        for p in list_posts(lang):
            if p.translation_group == translation_group:
                out[lang] = p.slug
                break
    return out
```

The language switcher and the `hreflang` alternates on the article page both fall out of this one function. No i18n framework, no routing config — just a dict lookup over files that already exist on disk.

## Drafts, for free

```python
def _is_draft(m: dict) -> bool:
    return bool(m.get("draft")) and not settings.DEBUG
```

`draft: true` in frontmatter hides a post in production but shows it locally — so I can write, preview, and sit on a post for weeks (several of these already are) without a staging environment or a publish button.

## Extending markdown costs nothing

```python
MD_EXTENSIONS = ["fenced_code", "tables", "toc", "footnotes", "pymdownx.tilde", "pymdownx.arithmatex"]
```

Tables, footnotes, and — via `pymdownx.arithmatex` — actual LaTeX math rendering, pinned once so a post published today never silently re-renders differently after a library upgrade.

## The door left open

The `PostMeta` dataclass is deliberately shaped like a future `Article` database row — same fields, same names. If this ever needs multiple authors, an editorial approval queue, or search across a large archive, there's a documented seam (`plans/racefeed-blog.md`) to swap the file loader for a real model behind the exact same interface. Every view already talks to `loader.list_posts()` / `loader.get_post()`, not to files directly — so that swap touches one module, not every template.

## When would you actually need a database?

Multiple authors stepping on each other's drafts. Comment moderation at scale. Full-text search across a few thousand posts. None of that is true yet. Building for it now would be optimizing a problem I don't have, at the cost of the one I do: writing and shipping without friction.

## What it unlocked

- Zero migrations, zero admin panel, zero ORM for content
- Editorial workflow that's just `git commit`
- Bilingual routing and hreflang from one frontmatter field
- A cache that invalidates itself by construction, not by TTL guesswork

The fastest CMS is the one you don't build.
