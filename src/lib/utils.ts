export function slugify(text: string) {
  return text
    .toString()
    .normalize("NFD") // split accented letters into base + accent
    .replace(/[\u0300-\u036f]/g, "") // remove accents
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-") // replace non-alphanumeric with dashes
    .replace(/^-+|-+$/g, ""); // remove leading/trailing dashes
}

export function createSafeUploadKey(transferId: string, filename: string, rootPrefix = "uploads") {
  const ext = filename.includes(".") ? filename.split(".").pop() : "";
  const date = new Date().toISOString().slice(0, 10);

  return `${rootPrefix}/${date}/${transferId}/${crypto.randomUUID()}${ext ? "." + ext : ""}`;
}

export async function fetchWithCredentials<T>(endpoint: string, options: RequestInit = {}) {
  const res = await fetch(endpoint, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  if (!res.ok) {
    const error = await res.text();
    throw new Error(`Failed to fetch: ${res.status} ${error}`);
  }

  return res.json() as T;
}

export function calculateExponentialBackoff(attempts: number, baseDelaySeconds: number) {
  return baseDelaySeconds ** attempts;
}

export function resolveNameInZip(key: string, objectPrefix: string, relativePath?: string): string {
  if (relativePath) {
    return relativePath;
  }

  // Remove leading and trailing slashes from the object prefix
  const currentObjectPrefix = objectPrefix.replace(/^\/+/, "").replace(/\/?$/, "/");
  const delimiterIndex = key.indexOf("__");
  const uploadedFileName = delimiterIndex >= 0 ? key.slice(delimiterIndex + 2) : "";

  return (
    uploadedFileName ||
    key.substring(currentObjectPrefix.length).replace(/^\/+/, "") ||
    key.split("/").pop() ||
    "file"
  );
}
