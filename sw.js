/* NecShare service worker — cache-first for local assets only.
   Firebase/CDN/API traffic always goes to the network. */
var CACHE = "necshare-v3";
var ASSETS = [
  "./",
  "index.html",
  "css/style.css",
  "js/app.js",
  "js/firebase-config.js",
  "manifest.webmanifest",
  "icons/icon-192.png",
  "icons/icon-512.png"
];

self.addEventListener("install", function (e) {
  e.waitUntil(
    caches.open(CACHE).then(function (c) { return c.addAll(ASSETS); })
      .then(function () { return self.skipWaiting(); })
  );
});

self.addEventListener("activate", function (e) {
  e.waitUntil(
    caches.keys().then(function (keys) {
      return Promise.all(keys.filter(function (k) { return k !== CACHE; })
        .map(function (k) { return caches.delete(k); }));
    }).then(function () { return self.clients.claim(); })
  );
});

self.addEventListener("fetch", function (e) {
  if (e.request.method !== "GET") return;
  var url = new URL(e.request.url);
  // Never intercept Firebase, CDN or any non-local traffic.
  if (url.origin !== self.location.origin) return;
  e.respondWith(
    caches.match(e.request).then(function (hit) {
      if (hit) return hit;
      return fetch(e.request).then(function (res) {
        // Only cache our own static assets, never anything else.
        var path = url.pathname;
        var names = ["index.html", "css/style.css", "js/app.js",
                     "js/firebase-config.js", "manifest.webmanifest",
                     "icons/icon-192.png", "icons/icon-512.png"];
        var isAsset = (path === "/") || names.some(function (n) {
          return path === "/" + n || path.endsWith("/" + n);
        });
        if (isAsset && res && res.ok) {
          var copy = res.clone();
          caches.open(CACHE).then(function (c) { c.put(e.request, copy); });
        }
        return res;
      }).catch(function () { return caches.match("index.html"); });
    })
  );
});
