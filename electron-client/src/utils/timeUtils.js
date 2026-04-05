/**
 * Parses a naive Eastern-Time datetime string (no timezone suffix, as stored
 * in MySQL) into a UTC millisecond timestamp, correctly handling DST.
 *
 * DST starts: 2nd Sunday of March at 02:00  → EDT (UTC-4)
 * DST ends:   1st Sunday of November at 02:00 → EST (UTC-5)
 */
export function etStringToUtcMs(etStr) {
  if (!etStr) return null;
  const iso = etStr.replace(" ", "T").replace(/(\.\d+)?$/, "");
  const [datePart, timePart = "00:00:00"] = iso.split("T");
  const [yr, mo, dy] = datePart.split("-").map(Number);
  const [hh, mm, ss] = timePart.split(":").map(Number);

  function nthSunday(year, month, n) {
    const d   = new Date(Date.UTC(year, month - 1, 1));
    const dow = d.getUTCDay();
    const first = dow === 0 ? 1 : 8 - dow;
    return first + (n - 1) * 7;
  }
  const dstStart = nthSunday(yr, 3,  2); // 2nd Sun March
  const dstEnd   = nthSunday(yr, 11, 1); // 1st Sun November

  let isEDT = false;
  if (mo > 3 && mo < 11) {
    isEDT = true;
  } else if (mo === 3) {
    isEDT = dy > dstStart || (dy === dstStart && hh >= 2);
  } else if (mo === 11) {
    isEDT = dy < dstEnd || (dy === dstEnd && hh < 2);
  }

  const offsetMs = (isEDT ? 4 : 5) * 3600 * 1000;
  const utcMs    = Date.UTC(yr, mo - 1, dy, hh, mm, ss || 0);
  return utcMs + offsetMs;
}

/**
 * Formats a naive ET datetime string as a human-readable ET time with AM/PM.
 * Parses the string components directly — no UTC round-trip — so the result
 * is immune to the machine's local timezone.
 * e.g. "2026-03-12 09:40:00"  →  "03/12, 09:40 AM"
 */
export function fmtEtString(etStr) {
  if (!etStr) return "—";
  const iso = String(etStr).replace(" ", "T").replace(/\.\d+$/, "");
  const [datePart, timePart = "00:00:00"] = iso.split("T");
  const [, mo, dy] = datePart.split("-").map(Number);
  const [hh, mm]   = timePart.split(":").map(Number);
  const ampm = hh >= 12 ? "PM" : "AM";
  const h12  = hh % 12 || 12;
  return `${String(mo).padStart(2, "0")}/${String(dy).padStart(2, "0")}, ${h12}:${String(mm).padStart(2, "0")} ${ampm}`;
}
