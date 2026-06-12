from django.urls import path
from django.views.generic import TemplateView

from profiles.views import identity_gate, onboarding

urlpatterns = [
    path("welcome/", TemplateView.as_view(
            template_name="profiles/welcome.html"
        ), name="account_signup"),
    path("gate/", identity_gate, name="identity_gate"),
    path('onboarding/<slug:token>', onboarding, name='onboarding'),
]

from allauth.account import views as account_views
from allauth.account.views import LogoutView, LoginView

urlpatterns += [
    # ---- Login / Logout ----
    path("fallback-login/", LoginView.as_view(), name="account_login"),
    path("logout/", LogoutView.as_view(), name="account_logout"),

    # ---- Account maintenance (allauth-managed) ----
    path("inactive/", account_views.account_inactive, name="account_inactive"),
    path("email/", account_views.email, name="account_email"),
    path("password/change/", account_views.password_change, name="account_change_password"),
    path("password/set/", account_views.password_set, name="account_set_password"),
    path("password/reset/", account_views.password_reset, name="account_reset_password"),
    path("password/reset/done/", account_views.password_reset_done, name="account_reset_password_done"),
    path(
        "password/reset/key/<uidb36>/<key>/",
        account_views.password_reset_from_key,
        name="account_reset_password_from_key",
    ),
]