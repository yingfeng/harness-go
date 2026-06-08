'use client';

import { SSEEvent } from '@/lib/api';
import { useRef, useEffect } from 'react';

interface ExecutionPanelProps {
  events: SSEEvent[];
  isExecuting: boolean;
  onClose: () => void;
}

export default function ExecutionPanel({ events, isExecuting, onClose }: ExecutionPanelProps) {
  const endRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [events]);

  return (
    <div className="fixed bottom-0 left-0 right-0 bg-white border-t border-gray-300 shadow-lg z-50">
      <div className="flex items-center justify-between px-4 py-2 bg-gray-50 border-b border-gray-200">
        <h3 className="text-sm font-semibold">
          Execution Log {isExecuting && <span className="text-blue-500 animate-pulse">(running...)</span>}
        </h3>
        <button className="text-gray-500 hover:text-gray-700" onClick={onClose}>
          ✕
        </button>
      </div>
      <div className="max-h-64 overflow-y-auto p-4 space-y-1 text-sm font-mono">
        {events.length === 0 && (
          <div className="text-gray-400 italic">No events yet. Click Execute to start.</div>
        )}
        {events.map((evt, i) => (
          <div key={i} className="flex flex-col gap-0 border-b border-gray-100 pb-1">
            <div className="flex gap-2">
              <span className="text-gray-500 font-semibold shrink-0 w-36">{evt.event}</span>
              <pre className="text-gray-700 whitespace-pre-wrap break-all flex-1 text-xs">
                {JSON.stringify(evt.data, null, 1)}
              </pre>
            </div>
          </div>
        ))}
        <div ref={endRef} />
      </div>
    </div>
  );
}
