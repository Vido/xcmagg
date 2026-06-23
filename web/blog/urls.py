from django.urls import path

from . import views

# No app_name: names are global (reverse("blog-article", ...)) so a future
# Article.get_absolute_url() reverses the same route with no URL churn.
urlpatterns = [
    path("", views.index, name="blog-index"),
    path("<slug:lang>/", views.archive, name="blog-archive"),
    path("<slug:lang>/<slug:slug>/", views.article, name="blog-article"),
]
