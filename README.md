# NecShare — Private Peer-to-Peer Sharing (PWA)

Chat, file sharing, voice and video calls with no accounts. Pairing happens
through a one-time 6-character code (or invite link). Firebase Realtime
Database is used **only for signaling** (codes, connection requests, WebRTC
handshake). Chats, files and call media travel **directly between the two
devices over WebRTC** — nothing is stored on any server.

## Setup (10 minutes, one time)

1. Go to https://console.firebase.google.com and open your project.
2. **Realtime Database** → Create Database → choose a region.
3. **Realtime Database → Rules** tab → paste the contents of
   `database.rules.json` → Publish.
4. Your web config is already in `js/firebase-config.js`.

Security model: there is no login, so peer IDs are 128-bit random values
stored in `localStorage`. Codes cannot be listed (only looked up when you
know them), expire after 5 minutes, and are deleted the moment a session
starts. Only someone holding a live code (or invite link) can reach you,
and every connection still needs your explicit Accept.

## Run locally

Serve the folder over HTTP (service workers need it):

```bash
cd ~/workspace/necshare
python3 -m http.server 8080
```

Open `http://localhost:8080` on two devices (or two browser windows).

## Deploy

Host the folder on any static HTTPS host (e.g. GitHub Pages). Then the PWA
is installable (Add to Home Screen). For an APK without a PC: open
https://www.pwabuilder.com on the phone, enter the site URL, download the
generated APK (Trusted Web Activity wrapper).

## How pairing works

1. You see **Your Code** (refreshes every 5 minutes, single use).
2. Share the code or the invite link (`?j=CODE` auto-fills it).
3. The other person enters it and taps **Connect** → you get a request.
4. You tap **Accept** → encrypted peer-to-peer session starts.

## Limits (honest)

- Calls use a free STUN server only. On very restrictive networks (symmetric
  NAT) calls may fail — a TURN server (e.g. Metered, Twilio) fixes that.
- Files are capped at 100 MB per transfer.
- If the data channel drops, tap **Reconnect** in the chat screen.
- Both devices need internet at the same time; there is no message history
  sync — chat lives in the live session.
