from django.urls import path

from . import views

app_name = "tools"

urlpatterns = [
    path("cycling-nutrition-calculator/", views.fuel_calculator, name="fuel-calculator"),
]
