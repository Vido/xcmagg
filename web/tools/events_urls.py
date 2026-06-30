from django.urls import path

from . import event_views, views

app_name = "events"

urlpatterns = [
    path("", views.calendar, name="calendar"),
    path("<slug:city_slug>/<slug:discipline>/", event_views.city_calendar, name="city-discipline"),
    path("<slug:city_slug>/",                   event_views.city_calendar, name="city"),
]
