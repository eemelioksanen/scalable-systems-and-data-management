import { useEffect, useRef } from 'react';

// Keys that appeared since the previous render, so a new row can flash once. The first render
// marks everything as seen.
export function useFresh(keys) {
  const seen = useRef(null);
  const fresh = seen.current ? new Set(keys.filter((k) => !seen.current.has(k))) : new Set();
  useEffect(() => {
    seen.current = new Set([...(seen.current ?? []), ...keys]);
  });
  return fresh;
}
