export async function verifyHmac(req: Request, secret: string) {
  const sig = req.headers.get("x-signature");
  const ts = req.headers.get("x-timestamp");
  if (!sig || !ts) throw new Response("Unauthorized", { status: 401 });

  // Skew check
  const now = Date.now();
  const t = Number.isFinite(+ts) ? +ts : Date.parse(ts);
  if (!Number.isFinite(t) || Math.abs(now - t) > 5 * 60 * 1000) {
    throw new Response("Unauthorized (timestamp)", { status: 401 });
  }

  const body = await req.clone().text();
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"]
  );
  const data = enc.encode(`${t}\n${body}`);
  const raw = await crypto.subtle.sign("HMAC", key, data);
  const digest = [...new Uint8Array(raw)].map((b) => b.toString(16).padStart(2, "0")).join("");

  if (digest !== sig) {
    throw new Response("Unauthorized (signature)", { status: 401 });
  }
}
