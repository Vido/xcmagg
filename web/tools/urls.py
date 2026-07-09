from django.urls import path
from django.views.generic import RedirectView

from . import views
from .redirects import TOOL_REDIRECTS

app_name = "tools"

urlpatterns = [
    path("cycling-nutrition-calculator/", views.nutrition_calculator, name="nutrition-calculator"),
    path("cycling-hydration-calculator/", views.hydration_calculator, name="hydration-calculator"),
    path("race-day-fuel-plan/", views.fuel_plan, name="fuel-plan"),
    path("stem-comparison/", views.stem_comparison, name="stem-comparison"),
    path("bike-gear-ratio-calculator/", views.gear_matrix, name="gear-matrix"),
    path("cycling-power-calculator/", views.gearftp, name="gearftp"),
] + [
    path(old, RedirectView.as_view(url=f"/tools/{new}", permanent=True))
    for old, new in TOOL_REDIRECTS.items()
]
