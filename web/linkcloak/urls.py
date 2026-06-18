from django.urls import path

from linkcloak.views import go_link

app_name = "linkcloak"

urlpatterns = [
    path("go2/<slug:slug>/", go_link, name="go"),
]
