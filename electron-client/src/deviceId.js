/** Stable device identifier for this app install (Electron userData or local fallback). */
let cached = null;

export async function getDeviceId() {
  if (cached) return cached;

  if (window.electronAPI?.getDeviceId) {
    cached = await window.electronAPI.getDeviceId();
    return cached;
  }

  let id = localStorage.getItem("tf_device_id");
  if (!id) {
    id = crypto.randomUUID();
    localStorage.setItem("tf_device_id", id);
  }
  cached = id;
  return id;
}
