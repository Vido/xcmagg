
from django.db import transaction
from django.urls import reverse
from django.http import HttpResponseBadRequest
from django.shortcuts import get_object_or_404, redirect, render

from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_POST

from nodes.models import Node
from engagement.models import Post
from engagement.selectors import PostSelector
from engagement.services import PostService, VoteService


def post_details(request, shortcode, slug):

    node = get_object_or_404(Node, shortcode=shortcode)
    # TODO: Check visibility

    if slug != node.slug:
        return redirect(
            reverse('post-details', args=[node.shortcode, node.slug]),
            permanent=True,
        )

    return render(
        request,
        'engagement/post_details.html',
        {
            'post': node.post,
            # 'photos': photos_for(post),
            'recent_posts': PostSelector.highlighted()
        },
    )

@login_required
@require_POST
def add_comment(request, shortcode):
    node = get_object_or_404(Node, shortcode=shortcode)

    body = request.POST.get('body', '').strip()
    if not body:
        return HttpResponseBadRequest('Comment cannot be empty')

    new_comment = PostService.add_comment(
        owner=request.user,
        parent=node,
        body=body,
    )

    return render(request, 'engagement/partials/_single_comment.html', {
        'comment': new_comment,
    })

@require_POST
def cast_vote(request, shortcode):

    if not request.user.is_authenticated:
        return HttpResponse(status=401)

    node = get_object_or_404(Node, shortcode=shortcode)

    action = request.POST.get('action').upper()
    if action not in ('UP', 'DOWN'):
        return HttpResponseBadRequest('Invalid vote action')

    VoteService.cast(
        user=request.user,
        node=node,
        action=action,
    )

    template = {
        'h': '_h_voting_widget.html',
        'v': '_v_voting_widget.html',
    }.get(request.GET.get('widget', 'h'))

    return render(
        request,
        f'engagement/partials/{template}',
        {
            'node': node,
            #'user_vote': action,
        },
    )