// API client — calls relative URLs that Next.js rewrites proxy to Go backend.

const API = ''; // relative, e.g. /api/models

export interface WorkflowNode {
  id: string;
  type: string;
  position: { x: number; y: number };
  data: NodeData;
}

export interface NodeData {
  label: string;
  nodeName: string;
  modelName?: string;
  instructions?: string;
  systemPrompt?: string;
  temperature?: number;
  condition?: string;
  transformCode?: string;
}

export interface WorkflowEdge {
  id: string;
  source: string;
  target: string;
  label?: string;
  sourceHandle?: string;
}

export interface WorkflowDef {
  id: string;
  name: string;
  nodes: WorkflowNode[];
  edges: WorkflowEdge[];
}

export interface ModelInfo {
  name: string;
  displayName: string;
  model: string;
  apiBase: string;
  hasApiKey: boolean;
  maxTokens: number;
  temperature: number;
}

export interface WorkflowTemplate {
  id: string;
  name: string;
  description: string;
  category: string;
  difficulty: string;
  nodeCount: number;
  workflow: WorkflowDef;
}

export interface SSEEvent {
  event: string;
  data: any;
}

export async function fetchTemplates(): Promise<WorkflowTemplate[]> {
  const res = await fetch('/api/templates');
  if (!res.ok) throw new Error('Failed to fetch templates');
  return res.json();
}

export async function fetchModels(): Promise<ModelInfo[]> {
  const res = await fetch('/api/models');
  if (!res.ok) throw new Error('Failed to fetch models');
  return res.json();
}

export async function updateAPIKey(name: string, apiKey: string): Promise<void> {
  const res = await fetch('/api/models/api-key', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, apiKey }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || 'Failed to update API key');
  }
}

export async function executeWorkflow(workflow: WorkflowDef): Promise<string> {
  const res = await fetch('/api/workflow/execute', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ workflow }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || 'Failed to execute workflow');
  }
  const data = await res.json();
  return data.executionId;
}

export function subscribeToStream(executionId: string, onEvent: (event: SSEEvent) => void): () => void {
  const url = `/api/workflow/stream?id=${encodeURIComponent(executionId)}`;
  console.log('[SSE] Connecting to', url);
  const eventSource = new EventSource(url);

  const events = [
    'workflow_started',
    'node_started',
    'node_completed',
    'node_failed',
    'state_update',
    'workflow_completed',
  ];

  events.forEach((eventName) => {
    eventSource.addEventListener(eventName, (e: MessageEvent) => {
      let data: any;
      try { data = JSON.parse(e.data); } catch { data = e.data; }
      console.log('[SSE]', eventName, data);
      onEvent({ event: eventName, data });
    });
  });

  eventSource.onerror = (e: Event | MessageEvent) => {
    // EventSource fires onerror on connection failures AND when parsing fails.
    // We only fire once to avoid duplicates.
    if (eventSource.readyState === EventSource.CLOSED) {
      console.warn('[SSE] Connection closed');
      onEvent({ event: 'error', data: { message: 'SSE connection closed unexpectedly' } });
    }
  };

  return () => {
    console.log('[SSE] Closing');
    eventSource.close();
  };
}
