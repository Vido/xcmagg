from django.conf import settings
from django.urls import path

from . import views

app_name = "events"

urlpatterns = [
    path("", views.calendar, name="calendar"),
]

if settings.DEBUG:
    from . import event_views
    urlpatterns += [
        path("<slug:city_slug>/<slug:discipline>/", event_views.city_discipline_calendar, name="city-discipline"),
        path("<slug:city_slug>/",                   event_views.city_calendar,            name="city"),
    ]
