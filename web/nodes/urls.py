from django.urls import path
from django.views.generic import TemplateView
from nodes.views import *

placeholder = TemplateView.as_view(template_name="_placeholder.html")

urlpatterns = [
    path("post/new/", placeholder, name="post-create"),
    path("post/<str:shortcode>/", placeholder, name="post-edit"),
    path("post/<str:shortcode>/<slug:slug>/", placeholder, name="post-details"),
    #path("catalog/<str:shortcode>/<slug:slug>/", placeholder, name="catalog-details"),
]