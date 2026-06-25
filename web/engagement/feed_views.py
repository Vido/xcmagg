from django.shortcuts import render
from django.core.paginator import Paginator
from django.db.models import Sum, Value
from django.db.models.functions import Coalesce
from django.utils import timezone
from datetime import timedelta
from engagement.selectors import PostSelector


def feed_view(request):
    sort = request.GET.get('sort', 'new')
    page_number = request.GET.get('page', 1)

    posts = PostSelector.base_qs()

    if sort == 'new':
        posts = posts.order_by('-node__published_at')

    elif sort == 'top':
        posts = (
            posts
            .annotate(vote_score=Coalesce(Sum("node__votes__value"), Value(0)))
            .order_by('-vote_score', '-node__published_at')
        )

    elif sort == 'hot':
        month_ago = timezone.now() - timedelta(days=30)
        posts = (
            posts
            .filter(node__published_at__gte=month_ago)
            .annotate(vote_score=Coalesce(Sum("node__votes__value"), Value(0)))
            .order_by('-vote_score', '-node__published_at')
        )
    
    # Pagination
    paginator = Paginator(posts, 3)  # 25 posts per page
    page_obj = paginator.get_page(page_number)
    
    context = {
        'posts': page_obj,
        'next_page': page_obj.next_page_number() if page_obj.has_next() else None,
        'has_next': page_obj.has_next(),
        'current_sort': sort,
    }

    if request.headers.get('HX-Request'):
        return render(request, 'engagement/partials/_post_list.html', context)

    context['recent_posts'] = (
        PostSelector.base_qs()
        .order_by('-node__published_at')[:5]
    )
    return render(request, 'engagement/feed.html', context)