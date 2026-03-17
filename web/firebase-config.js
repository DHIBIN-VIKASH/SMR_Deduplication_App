// firebase-config.js
// Firebase configuration and initialization

import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import { getAuth, GoogleAuthProvider, signInWithPopup, signOut, onAuthStateChanged }
  from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";
import { getFirestore, collection, addDoc, getDocs, query, orderBy, limit, where, serverTimestamp }
  from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";

// ─── YOUR FIREBASE PROJECT CONFIG ───────────────────────────────────────────
// Replace with your actual Firebase project values.
// Get these from: Firebase Console → Project Settings → Your App
// Docs: https://firebase.google.com/docs/web/setup
const firebaseConfig = {
  apiKey:            "AIzaSyPLACEHOLDER_REPLACE_ME",
  authDomain:        "your-project.firebaseapp.com",
  projectId:         "your-project-id",
  storageBucket:     "your-project.appspot.com",
  messagingSenderId: "000000000000",
  appId:             "1:000000000000:web:0000000000000000000000"
};
// ─────────────────────────────────────────────────────────────────────────────

let app, auth, db, provider;
let firebaseReady = false;

try {
  // Only init if the config has been replaced from placeholders
  if (!firebaseConfig.apiKey.includes("PLACEHOLDER")) {
    app = initializeApp(firebaseConfig);
    auth = getAuth(app);
    db = getFirestore(app);
    provider = new GoogleAuthProvider();
    firebaseReady = true;
    console.log("[Firebase] Initialized successfully.");
  } else {
    console.warn("[Firebase] Config not set – running in offline-only mode. " +
      "Update firebaseConfig in firebase-config.js to enable cloud features.");
  }
} catch (e) {
  console.error("[Firebase] Init error:", e);
}

export {
  auth, db, provider,
  firebaseReady,
  GoogleAuthProvider, signInWithPopup, signOut, onAuthStateChanged,
  collection, addDoc, getDocs, query, orderBy, limit, where, serverTimestamp
};
