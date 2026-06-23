"""File-backed blog post loader (option B).

Markdown files under ``blog/content/<lang>/<slug>.md`` are the source of truth.
This module reads + renders them, caching through Django's **filebased** cache
(``caches['blog']``, on disk — keeps rendered HTML out of RAM) keyed by file
mtime, so an edit invalidates naturally. No DB, no models — see
plans/racefeed-blog.md for the A-phase (Node/Article) door this leaves open.
``PostMeta`` deliberately mirrors the fields a future ``Article`` row would
expose, so the data source can be swapped behind the same shape.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import frontmatter
import markdown as md

from django.conf import settings
from django.core.cache import caches
from django.http import Http404

CONTENT_DIR = Path(settings.BASE_DIR) / "blog" / "content"
LANGS = ("en", "pt-br")
DEFAULT_LANG = "en"

# Pinned once so already-published posts never re-render differently.
MD_EXTENSIONS = ["fenced_code", "tables", "toc", "footnotes"]

# Keys embed file mtime/signature, so entries self-invalidate on edit; no expiry.
_CACHE_TIMEOUT = None


def _cache():
    return caches["blog"]


@dataclass(frozen=True)
class PostMeta:
    slug: str
    lang: str
    title: str
    description: str
    publish_date: date | None
    updated_date: date | None
    translation_group: str
    tags: tuple
    author: str = ""

    @property
    def is_updated(self) -> bool:
        return bool(self.updated_date and self.updated_date != self.publish_date)


def _path(lang: str, slug: str) -> Path:
    return CONTENT_DIR / lang / f"{slug}.md"


def _meta_from(slug: str, lang: str, m: dict) -> PostMeta:
    return PostMeta(
        slug=slug,
        lang=lang,
        title=m.get("title", slug),
        description=m.get("description", ""),
        publish_date=m.get("publish_date"),
        updated_date=m.get("updated_date"),
        translation_group=str(m.get("translation_group", slug)),
        tags=tuple(m.get("tags") or ()),
        author=m.get("author", ""),
    )


def _is_draft(m: dict) -> bool:
    return bool(m.get("draft"))


def _dir_signature(folder: Path) -> str:
    """Hash of (name, mtime) for every post — changes on add/edit/delete."""
    parts = [f"{p.name}:{p.stat().st_mtime_ns}" for p in sorted(folder.glob("*.md"))]
    return hashlib.md5("|".join(parts).encode()).hexdigest()


def list_posts(lang: str) -> list[PostMeta]:
    """Published posts for a language, newest first (cached by dir signature)."""
    folder = CONTENT_DIR / lang
    if not folder.is_dir():
        return []

    key = f"blog:list:{lang}:{_dir_signature(folder)}"
    cached = _cache().get(key)
    if cached is not None:
        return cached

    posts: list[PostMeta] = []
    for path in folder.glob("*.md"):
        post = frontmatter.load(path)
        if _is_draft(post.metadata):
            continue
        posts.append(_meta_from(path.stem, lang, post.metadata))
    posts.sort(key=lambda p: (p.publish_date or date.min), reverse=True)

    _cache().set(key, posts, _CACHE_TIMEOUT)
    return posts


def get_post(lang: str, slug: str) -> tuple[PostMeta, str]:
    """Rendered post body + metadata, or 404 (missing or draft) — cached by mtime."""
    path = _path(lang, slug)
    if not path.is_file():
        raise Http404("Post not found")

    key = f"blog:post:{lang}:{slug}:{path.stat().st_mtime_ns}"
    cached = _cache().get(key)
    if cached is not None:
        return cached

    post = frontmatter.load(path)
    if _is_draft(post.metadata):
        raise Http404("Post not found")

    meta = _meta_from(slug, lang, post.metadata)
    html = md.markdown(post.content, extensions=MD_EXTENSIONS)
    _cache().set(key, (meta, html), _CACHE_TIMEOUT)
    return meta, html


def siblings(translation_group: str) -> dict[str, str]:
    """{lang: slug} for every language that has this translation group.

    Drives the language switcher + hreflang alternates.
    """
    out: dict[str, str] = {}
    for lang in LANGS:
        for p in list_posts(lang):
            if p.translation_group == translation_group:
                out[lang] = p.slug
                break
    return out
