from django.conf import settings
from django.contrib import admin
from django.urls import path, include


urlpatterns = [
    path('admin_3dcx/', admin.site.urls),
    # path('accounts/', include('django.contrib.auth.urls')),
    path('accounts/', include('profiles.urls')),
    # path('accounts/', include('allauth.urls')),
    path('', include('linkcloak.urls')),
    path('tools/', include('tools.urls')),
]

from django.views.generic import TemplateView
placeholder = TemplateView.as_view(template_name="_placeholder.html")

# Catalog
from catalog.views import (
    home, category_profile, manufacturer_profile, catalog_details,
    category_list, manufacturer_list, catalog_list,
)
from catalog.form_views import new_item, new_catalog_item, edit_item
urlpatterns += [
    path("", home, name="home"),
    path("categories/", category_list, name="category-list"),
    path("manufacturers/", manufacturer_list, name="manufacturer-list"),
    path("catalog/", catalog_list, name="catalog-list"),
    path("manufacturer/<slug:brand>/<slug:shortcode>/<slug:slug>/", catalog_details, name="catalog-details"),
    path("manufacturer/<slug:slug>", manufacturer_profile, name="manufacturer-profile"),
    path("category/<slug:slug>", category_profile, name="category-profile"),
    path("inventory/new/", new_item, name="new-item"),
    path("catalog/new/", new_catalog_item, name="new-catalog-item"),
    path("catalog/<slug:shortcode>/<slug:slug>/", edit_item, name="edit-item"),
]

from catalog.form_views import autocomplete_category, autocomplete_manufacturer, autocomplete_catalog_node
urlpatterns += [
    path("lookups/category/", autocomplete_category, name="category-lookup"),
    path("lookups/manufacturer/", autocomplete_manufacturer, name="manufacturer-lookups"),
    path("lookups/catalog-node/", autocomplete_catalog_node, name="catalog-lookups"),
]

# Posts
from engagement.views import post_details
from engagement.form_views import  new_post, edit_post
from engagement.feed_views import  feed_view

urlpatterns += [
    path("feed/", feed_view, name="feed"),
    path("feed/<slug:shortcode>/<slug:slug>/", placeholder, name="post-subfeed"),
    path("post/new/", new_post, name="new-post"),
    path("post/<slug:shortcode>/<slug:slug>/", post_details, name="post-details"),
    path("post/<slug:shortcode>/", edit_post, name="edit-post"),
]

# Media
urlpatterns += [
    path("photos/upload/<slug:shortcode>/", placeholder, name="upload-photos"),
    path("photos/<slug:shortcode>/", placeholder, name="photos"),
]

# Engagement
from engagement.views import cast_vote, add_comment, cast_rating
urlpatterns += [
    path("vote/<slug:shortcode>/", cast_vote, name="cast-vote"),
    path("rate/<slug:shortcode>/", cast_rating, name="cast-rating"),
    path("comments/new/<slug:shortcode>/", add_comment, name="add-comment"),
    path("comments/<slug:shortcode>/", placeholder, name="comments"),
]

# Inventory
from catalog.views import inventory_details, inventory_page
urlpatterns += [
    path("@<str:username>/item/<slug:shortcode>/<slug:slug>/", inventory_details, name="inventory-details"),
    path("@<str:username>/inventory/", inventory_page, name="user-inventory"),
    path("@<str:username>/wishlist/", placeholder, name="user-wishlist"),
]

# MagicLink QR
from magiclink.views import generate_qr, magic_login
urlpatterns += [
    path("welcome/", TemplateView.as_view(
            template_name="profiles/welcome.html"
        ), name="welcome"),
    path('magic/qr/', generate_qr, name='generate-qr'),
    path('magic/login/<slug:token>/', magic_login, name='magic-login'),
]

if settings.DEBUG:
    from django.conf.urls.static import static
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)


handler429 = 'magiclink.views.ratelimit_handler'