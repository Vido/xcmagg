from django.urls import path
from django.views.generic import TemplateView
from media.views import *

placeholder = TemplateView.as_view(template_name="_placeholder.html")

urlpatterns = [
    path("photos/<str:shortcode>/", photos, name="photos"),
    path("photos/<str:shortcode>/delete/", photo_delete, name="photo-delete"),
]