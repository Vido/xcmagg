from django.shortcuts import render
from django.core.paginator import Paginator
from django.db.models import Count
from django.utils import timezone
from datetime import timedelta
from engagement.models import Post
from engagement.selectors import PostSelector
PostSelector

def feed_view(request):
    """
    Main feed view with infinite scroll support.
    Supports sorting by: hot, new, top
    """
    
    # Get sort parameter (default: hot)
    sort = request.GET.get('sort', 'hot')
    page_number = request.GET.get('page', 1)
    
    # Base queryset
    posts = PostSelector.base_qs()
    #posts = Post.objects.all()
    
    # Apply sorting
    if sort == 'new':
        posts = posts.order_by('-node__published_at')
    
    elif sort == 'top':
        #posts = posts.order_by('-node__score', '-node__published_at')
        posts = posts.order_by('-node__published_at')
    
    elif sort == 'hot':
        # Hot algorithm: recent posts with good engagement
        week_ago = timezone.now() - timedelta(days=7)
        posts = posts.filter(
            node__published_at__gte=week_ago
        #).order_by('-node__score', '-node__published_at')
        ).order_by('-node__published_at')
    
    # Pagination
    paginator = Paginator(posts, 3)  # 25 posts per page
    page_obj = paginator.get_page(page_number)
    
    context = {
        'posts': page_obj,
        'next_page': page_obj.next_page_number() if page_obj.has_next() else None,
        'has_next': page_obj.has_next(),
        'current_sort': sort,
    }
    
    # For HTMX requests (infinite scroll), return only the post list
    if request.headers.get('HX-Request'):
        print("HTMX")
        return render(request, 'engagement/partials/_post_list.html', context)

    print("GET-HTML")
    # For regular requests, return full page
    return render(request, 'engagement/feed.html', context)