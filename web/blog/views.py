from django.http import Http404
from django.shortcuts import redirect, render

from . import loader


def index(request):
    """/blog/ → default language archive."""
    return redirect("blog-archive", lang=loader.DEFAULT_LANG)


def archive(request, lang):
    """/blog/<lang>/ → list of posts in that language."""
    if lang not in loader.LANGS:
        raise Http404("Unknown language")
    return render(
        request,
        "blog/list.html",
        {"lang": lang, "posts": loader.list_posts(lang)},
    )


def article(request, lang, slug):
    """/blog/<lang>/<slug>/ → a single rendered post."""
    if lang not in loader.LANGS:
        raise Http404("Unknown language")
    post, body = loader.get_post(lang, slug)
    return render(
        request,
        "blog/article.html",
        {
            "lang": lang,
            "post": post,
            "body": body,
            "siblings": loader.siblings(post.translation_group),
            "sidebar_posts": loader.list_posts(lang),
        },
    )
