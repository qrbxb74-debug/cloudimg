const CACHE_NAME = 'wallpepersphere-v1';
const IMAGE_CACHE = 'wallpepersphere-images-v1';

const STATIC_ASSETS = [
    '/static/home.css',
    '/static/wallpeperspheredark.jpg',
    '/static/wallpeperspherelogo.jpg',
    '/static/icons/search.png',
    '/static/icons/filter.png',
    '/static/icons/upload.png',
    '/static/icons/download.png',
    '/static/icons/heart.png',
    '/static/icons/eye.png'
];

self.addEventListener('install', event => {
    event.waitUntil(
        caches.open(CACHE_NAME).then(cache => {
            return cache.addAll(STATIC_ASSETS);
        })
    );
});

self.addEventListener('fetch', event => {
    const url = new URL(event.request.url);

    // Cache Images (Uploads) - Cache First, Fallback to Network
    // This significantly reduces data consumption for viewed images
    if (url.pathname.startsWith('/uploads/') || url.pathname.startsWith('/static/avatars/')) {
        event.respondWith(
            caches.open(IMAGE_CACHE).then(cache => {
                return cache.match(event.request).then(response => {
                    return response || fetch(event.request).then(networkResponse => {
                        // Only cache valid responses
                        if (networkResponse.ok) {
                            cache.put(event.request, networkResponse.clone());
                        }
                        return networkResponse;
                    });
                });
            })
        );
        return;
    }
});