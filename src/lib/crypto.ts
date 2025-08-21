/**
 * This file created and verifies HMAC signatures on edge runtime
 */


async function importKey(secret: string) {
  const enc = new TextEncoder();
  return await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"]
  );
}

export async function verifyHmac(req: Request, secret: string) {
  const sig = req.headers.get("x-signature");
  const ts = req.headers.get("x-timestamp");
  if (!sig || !ts) {
    throw new Response("Unauthorized", { status: 401 });
  }

  // Skew check
  const now = Date.now();
  const t = Number.isFinite(+ts) ? +ts : Date.parse(ts);
  if (!Number.isFinite(t) || Math.abs(now - t) > 5 * 60 * 1000) {
    throw new Response("Unauthorized (timing)", { status: 401 });
  }

  const enc  = new TextEncoder();
  const body = await req.clone().text();
  const key  = await importKey(secret);

  const data = enc.encode(`${t}\n${body}`);
  const raw = await crypto.subtle.sign("HMAC", key, data);
  
  const digest = [...new Uint8Array(raw)].map((b) => b.toString(16).padStart(2, "0")).join("");

  if (digest !== sig) {
    throw new Response("Unauthorized (signature)", { status: 401 });
  }
}

export async function createHmacSha256Hex(payload: string, secret: string) {
  // Create HMAC SHA256 hash on edge runtime
  const enc = new TextEncoder();
  const data = enc.encode(payload);

  const key = await importKey(secret);
  const raw = await crypto.subtle.sign("HMAC", key, data);

  return [...new Uint8Array(raw)].map((b) => b.toString(16).padStart(2, "0")).join("");
}
