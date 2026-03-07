const CACHE_NAME = 'wallpepersphere-v1';
const IMAGE_CACHE = 'wallpepersphere-images-v1';
const API_CACHE = 'wallpepersphere-api-v1';

const STATIC_ASSETS = [
    '/static/home.css',
    '/static/wallpeperspheredark.jpg',
    '/static/wallpepersphere.jpg',
    '/static/hero.webp',
    '/static/icons/search.png',
    '/static/icons/filter.png',
    '/static/icons/upload.png',
    '/static/icons/download.png',
    '/static/icons/heart.png',
    '/static/icons/eye.png',
    '/static/icons/back.png',
    '/static/icons/next.png',
    '/static/icons/expand.png',
    '/static/icons/share.png',
    '/static/icons/settings.png',
    '/static/icons/home.png',
    '/static/icons/picture.png',
    '/static/icons/bookmark.png',
    '/static/icons/vertical.png',
    '/static/icons/horizontal.png',
    '/static/icons/stop-square.png',
    '/static/icons/apps.png',
    '/static/icons/email.png',
    '/static/icons/pencil.png',
    '/static/icons/bell.png',
    '/static/icons/domain.png',
    '/static/icons/instagram.png',
    '/static/icons/twitter.png'
];

self.addEventListener('install', event => {
    self.skipWaiting();
    event.waitUntil(
        caches.open(CACHE_NAME).then(cache => {
            console.log('Service Worker: Caching static assets');
            const cachePromises = STATIC_ASSETS.map(asset => {
                // add() fetches and caches.
                // We catch individual errors so one failed asset doesn't break the whole cache.
                return cache.add(asset).catch(err => {
                    console.warn(`Service Worker: Failed to cache ${asset}`, err);
                });
            });
            return Promise.all(cachePromises);
        })
    );
});

self.addEventListener('activate', event => {
    event.waitUntil(clients.claim());
});

self.addEventListener('fetch', event => {
    const url = new URL(event.request.url);

    // Strategy: Cache First, Fallback to Network for all cached assets.
    const isStaticAsset = STATIC_ASSETS.includes(url.pathname);
    const isImage = event.request.destination === 'image';

    if (isStaticAsset || isImage) {
        const cacheName = isImage ? IMAGE_CACHE : CACHE_NAME;
        event.respondWith(
            caches.open(cacheName).then(cache => {
                return cache.match(event.request).then(response => {
                    // Return from cache if found, otherwise fetch from network.
                    const fetchPromise = fetch(event.request).then(networkResponse => {
                        // If fetch is successful, cache the new response.
                        if (networkResponse && networkResponse.status === 200) {
                            cache.put(event.request, networkResponse.clone());
                        }
                        return networkResponse;
                    });
                    return response || fetchPromise;
                });
            })
        );
        return;
    }

    // Cache API Requests (Network First, Fallback to Cache)
    // This ensures the list of images (first 10+) is available offline
    if (url.pathname.startsWith('/api/')) {
        event.respondWith(
            caches.open(API_CACHE).then(cache => {
                return fetch(event.request).then(networkResponse => {
                    if (networkResponse && networkResponse.status === 200) {
                        cache.put(event.request, networkResponse.clone());
                    }
                    return networkResponse;
                }).catch(() => cache.match(event.request));
            })
        );
    }
});