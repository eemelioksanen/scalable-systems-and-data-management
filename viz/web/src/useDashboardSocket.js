import { useEffect, useReducer, useRef, useState } from 'react';
import { applyDelta, applySeries, applySnapshot, initialState } from './state.js';

function reducer(state, msg) {
  switch (msg.type) {
    case 'snapshot':
      return applySnapshot(state, msg);
    case 'delta':
      return applyDelta(state, msg);
    case 'series':
      return applySeries(state, msg);
    default:
      return state;
  }
}

function socketUrl() {
  const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws';
  return `${scheme}://${window.location.host}/ws`;
}

// Connects to the backend, reconnecting with backoff from 1 s to 30 s. Every (re)connect starts
// from a snapshot, so nothing missed while disconnected has to be replayed.
export function useDashboardSocket(selected) {
  const [state, dispatch] = useReducer(reducer, initialState);
  const [status, setStatus] = useState('connecting');
  const socketRef = useRef(null);
  const selectedRef = useRef(selected);

  useEffect(() => {
    let closed = false;
    let retryMs = 1000;
    let timer = null;

    function connect() {
      const ws = new WebSocket(socketUrl());
      socketRef.current = ws;
      ws.onopen = () => {
        retryMs = 1000;
        setStatus('live');
        if (selectedRef.current) ws.send(JSON.stringify({ type: 'select', symbol: selectedRef.current }));
      };
      ws.onmessage = (event) => dispatch(JSON.parse(event.data));
      ws.onclose = () => {
        socketRef.current = null;
        if (closed) return;
        setStatus('reconnecting');
        timer = setTimeout(connect, retryMs);
        retryMs = Math.min(retryMs * 2, 30000);
      };
    }

    connect();
    return () => {
      closed = true;
      clearTimeout(timer);
      socketRef.current?.close();
    };
  }, []);

  useEffect(() => {
    selectedRef.current = selected;
    const ws = socketRef.current;
    if (selected && ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'select', symbol: selected }));
    }
  }, [selected]);

  return { state, status };
}
