from django import forms

from django.urls import reverse
from django.db import transaction
from django.contrib import messages
from django.shortcuts import (
    render,
    redirect,
    get_object_or_404
)
from django.contrib.auth.decorators import login_required
from django.contrib.admin.views.decorators import staff_member_required
from django.views.decorators.http import require_http_methods
from django.http import HttpResponse, HttpResponseForbidden, QueryDict

from nodes.models import Node, NodeKind
from catalog.models import (
    Category,
    Manufacturer,
    Item,
    RetailerLink
)
from media.models import Photo
from media.services import PhotoService


class NodeForm(forms.ModelForm):
    class Meta:
        model = Node
        fields = [
            "title",
            "visibility",
            "votes_allowed",
            "comments_allowed",
        ]


class ItemForm(forms.ModelForm):
    class Meta:
        model = Item
        fields = [
            "description",
            "category",
            "manufacturer",
            "catalog_node",
            "acquired_at",
            "cost",
            "is_listed",
            "accepts_trade",
            "price",
        ]

RetailerLinkFormSet = forms.inlineformset_factory(
    Node,
    RetailerLink,
    fields=["text", "url", "is_affiliate", "promo_code"],
    extra=1,
    can_delete=True,
)


def htmx_redirect(request, obj):
    url = obj.get_absolute_url()
    if obj.is_draft:
        messages.info(request, "Draft Saved.")
        url = obj.get_edit_url()
    else:
        messages.success(request, "Nice Gear! Thank you.")

    #return HttpResponse(
    #    "Redirecting...",
    #   headers={"HX-Redirect": url},
    #)
    return redirect(url)

@login_required
@require_http_methods(["GET", "POST"])
def new_item(request):

    context = {
        "node_form": NodeForm(),
        "item_form": ItemForm(),
        "title": 'New Item',
        "item": None,
        "new": True,
    }

    if request.method == "GET":
        return render(request, "catalog/item_editor.html", context)

    node_form = NodeForm(request.POST)
    item_form = ItemForm(request.POST)
    if not (node_form.is_valid() and item_form.is_valid()):
        context["node_form"] = node_form
        context["item_form"] = item_form
        return render(request, "catalog/item_editor.html", context)

    with transaction.atomic():
        node = node_form.save(commit=False)
        if node.is_draft:
            node.is_draft = request.POST.get('publish') == "draft"
        node.owner = request.user
        node.kind = NodeKind.INVENTORY_ITEM
        node.save(send_signals=False)

        PhotoService.add(
            node=node,
            add_json=request.POST.get('new_photos'),
            request_files=request.FILES.getlist("photos")
        )

        item = item_form.save(commit=False)
        item.node = node
        item.save()

    return htmx_redirect(request, item)


@staff_member_required
@require_http_methods(["GET", "POST"])
def new_catalog_item(request):

    initial = {}
    cat_slug = request.GET.get("category")
    man_slug = request.GET.get("manufacturer")
    if cat_slug:
        initial["category"] = Category.objects.filter(node__slug=cat_slug).first()
    if man_slug:
        initial["manufacturer"] = Manufacturer.objects.filter(node__slug=man_slug).first()

    context = {
        "node_form": NodeForm(),
        "item_form": ItemForm(initial=initial),
        "title": "New Catalog Item",
        "item": None,
        "new": True,
    }

    if request.method == "GET":
        return render(request, "catalog/item_editor.html", context)

    node_form = NodeForm(request.POST)
    item_form = ItemForm(request.POST)
    context["node_form"] = node_form
    context["item_form"] = item_form

    if not (node_form.is_valid() and item_form.is_valid()):
        return render(request, "catalog/item_editor.html", context)

    publishing = request.POST.get("publish") != "draft"
    if publishing and not item_form.cleaned_data.get("manufacturer"):
        messages.error(request, "A catalog item needs a manufacturer before publishing.")
        return render(request, "catalog/item_editor.html", context)

    with transaction.atomic():
        node = node_form.save(commit=False)
        node.is_draft = not publishing
        node.owner = request.user
        node.kind = NodeKind.CATALOG_ITEM
        node.save(send_signals=False)

        PhotoService.add(
            node=node,
            add_json=request.POST.get('new_photos'),
            request_files=request.FILES.getlist("photos")
        )

        item = item_form.save(commit=False)
        item.node = node
        item.save()

    return htmx_redirect(request, item)


@login_required
@require_http_methods(["GET", "POST", "DELETE"])
def edit_item(request, shortcode, slug=None):
    node = get_object_or_404(
        Node,
        shortcode=shortcode,
        kind__in=[NodeKind.CATALOG_ITEM, NodeKind.INVENTORY_ITEM],
    )

    if node.owner != request.user:
        return HttpResponseForbidden()

    if request.method == "DELETE":
        with transaction.atomic():
            #node.is_deleted = True
            #node.save(update_fields=["is_deleted"])
            node.delete()
            return HttpResponse(
                '',
                headers={"HX-Redirect":
                    reverse('user-inventory', 
                        kwargs={'username': request.user.username})
                    },
            )

    node_form = NodeForm(instance=node)
    item_form = ItemForm(instance=node.item)
    formset = RetailerLinkFormSet(
        request.POST or None,
        instance=node,
        prefix="retailer")

    if request.method == "POST":
        node_form = NodeForm(request.POST, instance=node)
        item_form = ItemForm(request.POST, instance=node.item)

        if (node_form.is_valid() and
            item_form.is_valid() and 
            formset.is_valid()):

            with transaction.atomic():
                node = node_form.save(commit=False)
                node.is_draft = request.POST.get('publish') == "draft"
                node.save()

                formset.instance = item
                formset.save()

                item = item_form.save()

                PhotoService.delete(
                    node=node,
                    deleted_json=request.POST.get('deleted_photos')
                )
                PhotoService.update(
                    node=node,
                    update_json=request.POST.get('current_photos'),
                    add_json=request.POST.get('new_photos'),
                    request_files=request.FILES.getlist('photos')
                )

            return htmx_redirect(request, item)

    return render(
        request,
        'catalog/item_editor.html',
        {
            'node_form': node_form,
            'item_form': item_form,
            'retailerlink_formset': formset,
            'item': node.item,
            'photos': PhotoService.json_photos(node),
            "title": f'Edit { node.get_kind_display() }',
            "new": node.is_draft,
        },
    )

@login_required
def autocomplete_category(request):
    q = request.GET.get("q", '')
    results = Category.objects.filter(title__icontains=q)[:10]
    return render(request, "catalog/autocomplete/category.html", {"results": results})

@login_required
def autocomplete_manufacturer(request):
    q = request.GET.get("q", "")
    results = Manufacturer.objects.filter(title__icontains=q)[:10]
    return render(request, "catalog/autocomplete/manufacturer.html", {"results": results})

@login_required
def autocomplete_catalog_node(request):
    q = request.GET.get("q", "")
    results = Node.objects.filter(
        kind=NodeKind.CATALOG_ITEM,
        title__icontains=q,
    )[:10]
    return render(request, "catalog/autocomplete/catalog_node.html", {"results": results})


