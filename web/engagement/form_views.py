from django import forms
from django.db import transaction
from django.shortcuts import (
    render, redirect,
    get_object_or_404,
)
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
from django.contrib import messages

from django.http import HttpResponse, HttpResponseForbidden
from django.urls import reverse

from engagement.models import Post
from engagement.services import RatingService
from engagement.selectors import RatingSelector
from nodes.models import Node, NodeKind, Visibility
from media.services import PhotoService


class NodeForm(forms.ModelForm):
    class Meta:
        model = Node
        fields = [
            "title",
            "parent",
            "visibility",
            "votes_allowed",
            "comments_allowed",
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        parent = self.fields["parent"]
        parent.required = False if self.instance.pk else True
        # Key the field on shortcode so option values / submitted values are
        # shortcodes, never raw DB ids.
        parent.to_field_name = "shortcode"
        parent.queryset = Node.objects.filter(
            kind__in={
                NodeKind.CATEGORY,
                NodeKind.MANUFACTURER,
                NodeKind.CATALOG_ITEM,
                # NodeKind.INVENTORY_ITEM,
            },
            visibility=Visibility.PUBLIC,
            posts_allowed=True,
        )

    def clean_parent(self):
        parent = self.cleaned_data.get("parent")

        # If updating and parent wasn't sent, keep existing one
        if self.instance.pk and parent is None:
            return self.instance.parent

        return parent

class PostForm(forms.ModelForm):
    class Meta:
        model = Post
        fields = [
            "body",
        ]
        widgets = {
            "body": forms.Textarea(
                attrs={"rows": 5, "placeholder": "Write your post (Markdown supported)..."}
            ),
        }

def htmx_redirect(request, obj):
    url = obj.get_absolute_url()
    if obj.is_draft:
        messages.info(request, "Draft Saved.")
        url = obj.get_edit_url()
    else:
        messages.success(request, "Nice Post! Thank you.")

    #return HttpResponse(
    #    "Redirecting...",
    #    headers={"HX-Redirect": url},
    #)
    return redirect(url)

@login_required
@require_http_methods(["GET", "POST"])
def new_post(request):

    initial = {}
    parent_code = request.GET.get("parent")
    if parent_code:
        parent_node = Node.objects.filter(shortcode=parent_code).first()
        if parent_node:
            initial["parent"] = parent_node

    context = {
        "node_form": NodeForm(initial=initial),
        "post_form": PostForm(),
        "title": 'New Post',
        "post": None,
        "new": True,
        "rating": None,
    }

    if request.method == "GET":
        return render(request, "engagement/post_editor.html", context)

    node_form = NodeForm(request.POST)
    post_form = PostForm(request.POST)

    if not (node_form.is_valid() and post_form.is_valid()):
        context["node_form"] = node_form
        context["post_form"] = post_form
        print(node_form.errors)
        print(post_form.errors)
        return render(request, "engagement/post_editor.html", context)

    # A post under a catalog item is a Review and carries a star rating.
    parent = node_form.cleaned_data.get("parent")
    is_review = bool(parent and parent.kind == NodeKind.CATALOG_ITEM)
    publishing = request.POST.get("publish") != "draft"
    rating_val = request.POST.get("rating")
    if is_review and publishing and not rating_val:
        messages.error(request, "A review needs a star rating before publishing.")
        context["node_form"] = node_form
        context["post_form"] = post_form
        return render(request, "engagement/post_editor.html", context)

    with transaction.atomic():
        # Save node
        node = node_form.save(commit=False)
        node.owner = request.user
        node.kind = NodeKind.POST
        node.is_draft = request.POST.get('publish') == "draft"
        node.save(send_signals=False)

        # Optional: handle photos attached to post
        PhotoService.add(
            node=node,
            add_json=request.POST.get('new_photos'),
            request_files=request.FILES.getlist("photos")
        )

        # Save post
        post = post_form.save(commit=False)
        post.node = node
        post.save()

        RatingService.cast_review(
            user=request.user, parent_node=node.parent, value=rating_val
        )

    return htmx_redirect(request, post)


@login_required
@require_http_methods(["GET", "POST", "DELETE"])
def edit_post(request, shortcode):
    node = get_object_or_404(Node, shortcode=shortcode,
        kind__in=[NodeKind.POST, NodeKind.COMMENT])

    if node.owner != request.user:
        return HttpResponseForbidden()

    if request.method == "DELETE":
        with transaction.atomic():
            node.delete()

        messages.warning(request,
            "Draft deleted." if node.is_draft else "Post deleted.")

        return HttpResponse("Redirecting...",
            headers={
            # TODO
            "HX-Redirect": reverse("user-inventory",
                kwargs={"username": request.user.username}
            )},
        )

    node_form = NodeForm(instance=node)
    post_form = PostForm(instance=node.post)

    if request.method == "POST":

        node_form = NodeForm(request.POST, instance=node)
        post_form = PostForm(request.POST, instance=node.post)

        if node_form.is_valid() and post_form.is_valid():

            with transaction.atomic():
                node = node_form.save(commit=False)
                if node.is_draft:
                    node.is_draft = request.POST.get('publish') == "draft"
                node.save()

                post = post_form.save()
                PhotoService.delete(
                    node=node,
                    deleted_json=request.POST.get("deleted_photos")
                )

                PhotoService.update(
                    node=node,
                    update_json=request.POST.get("current_photos"),
                    add_json=request.POST.get("new_photos"),
                    request_files=request.FILES.getlist("photos")
                )

                RatingService.cast_review(
                    user=request.user,
                    parent_node=node.parent,
                    value=request.POST.get("rating"),
                )

            return htmx_redirect(request, post)


    return render(
        request,
        "engagement/post_editor.html",
        {
            "node_form": node_form,
            "post_form": post_form,
            "post": node.post,
            "photos": PhotoService.json_photos(node),
            "title": f'Edit { node.get_kind_display() }',
            "new": node.is_draft,
            "rating": RatingSelector.user_rating(node.parent, request.user),
        },
    )