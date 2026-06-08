'use client';

import { useState, useEffect, useCallback } from 'react';
import WorkflowCanvas from '@/components/WorkflowCanvas';
import ConfigPanel from '@/components/ConfigPanel';
import ExecutionPanel from '@/components/ExecutionPanel';
import {
  WorkflowNode,
  WorkflowEdge,
  WorkflowDef,
  WorkflowTemplate,
  ModelInfo,
  SSEEvent,
  executeWorkflow,
  subscribeToStream,
  fetchModels,
  fetchTemplates,
} from '@/lib/api';
import { Node, Edge } from '@xyflow/react';
import toast, { Toaster } from 'react-hot-toast';

export default function Home() {
  const [selectedNode, setSelectedNode] = useState<WorkflowNode | null>(null);
  const [workflowNodes, setWorkflowNodes] = useState<WorkflowNode[]>([]);
  const [workflowEdges, setWorkflowEdges] = useState<WorkflowEdge[]>([]);
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [templates, setTemplates] = useState<WorkflowTemplate[]>([]);
  const [isExecuting, setIsExecuting] = useState(false);
  const [execEvents, setExecEvents] = useState<SSEEvent[]>([]);
  const [showExecPanel, setShowExecPanel] = useState(false);
  const [showConfigPanel, setShowConfigPanel] = useState(true);

  // Template-driven canvas reset
  const [canvasKey, setCanvasKey] = useState(0);
  const [canvasNodes, setCanvasNodes] = useState<Node[] | undefined>(undefined);
  const [canvasEdges, setCanvasEdges] = useState<Edge[] | undefined>(undefined);

  const loadModels = useCallback(async () => {
    try {
      const list = await fetchModels();
      setModels(list);
      const hasKey = list.some((m) => m.hasApiKey);
      if (!hasKey && list.length > 0) {
        toast('Configure at least one API key in the settings panel', { icon: '🔑' });
      }
    } catch (e: any) {
      console.error('fetchModels failed:', e);
      toast.error(`Backend not reachable: ${e.message}. Ensure Go server is running on port 8080.`);
    }
  }, []);

  const loadTemplates = useCallback(async () => {
    try {
      const list = await fetchTemplates();
      setTemplates(list);
      // Check URL for ?template=xxx
      const params = new URLSearchParams(window.location.search);
      const tplId = params.get('template');
      if (tplId && list.length > 0) {
        const tpl = list.find((t) => t.id === tplId);
        if (tpl) applyTemplate(tpl);
      }
    } catch { /* templates are optional */ }
  }, []);

  useEffect(() => {
    loadModels();
    loadTemplates();
  }, [loadModels, loadTemplates]);

  const handleWorkflowChange = useCallback((nodes: WorkflowNode[], edges: WorkflowEdge[]) => {
    setWorkflowNodes(nodes);
    setWorkflowEdges(edges);
  }, []);

  const applyTemplate = useCallback((tpl: WorkflowTemplate) => {
    const wf = tpl.workflow;
    const rfNodes: Node[] = wf.nodes.map((n) => ({
      id: n.id,
      type: n.type,
      position: { x: n.position.x || 0, y: n.position.y || 0 },
      data: { ...n.data },
    }));
    const rfEdges: Edge[] = wf.edges.map((e) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      sourceHandle: e.sourceHandle || undefined,
      label: e.label || undefined,
    }));
    // Force canvas remount with new nodes/edges
    setCanvasNodes(rfNodes);
    setCanvasEdges(rfEdges);
    setCanvasKey((k) => k + 1);
    toast.success(`Loaded template: ${tpl.name}`);
  }, []);

  const handleExecute = useCallback(async () => {
    const hasUsableModel = models.some((m) => m.hasApiKey);
    if (!hasUsableModel) {
      toast.error('Configure at least one API key in the settings panel');
      return;
    }

    const wf: WorkflowDef = {
      id: `wf-${Date.now()}`,
      name: 'My Workflow',
      nodes: workflowNodes,
      edges: workflowEdges,
    };

    setIsExecuting(true);
    setExecEvents([]);
    setShowExecPanel(true);

    try {
      const execId = await executeWorkflow(wf);
      const unsubscribe = subscribeToStream(execId, (event: SSEEvent) => {
        setExecEvents((prev) => [...prev, event]);
        if (event.event === 'workflow_completed') {
          setIsExecuting(false);
          unsubscribe();
          if (event.data?.status === 'completed') {
            toast.success('Workflow completed');
          } else {
            toast.error(`Workflow ${event.data?.status}: ${event.data?.error || 'Unknown error'}`);
          }
        } else if (event.event === 'error') {
          setIsExecuting(false);
          unsubscribe();
          toast.error(event.data?.message || 'Execution error');
        }
      });
    } catch (err: any) {
      setIsExecuting(false);
      toast.error(err.message || 'Failed to start execution');
    }
  }, [models, workflowNodes, workflowEdges]);

  return (
    <div className="h-screen flex flex-col">
      <Toaster position="top-right" />
      <header className="flex items-center justify-between px-4 py-2 bg-gray-900 text-white border-b border-gray-700">
        <div className="flex items-center gap-3">
          <h1 className="text-lg font-bold">Open Agent Builder</h1>
          <span className="text-xs bg-blue-600 px-2 py-0.5 rounded">harness-go</span>
          {templates.length > 0 && (
            <select
              className="ml-4 bg-gray-800 text-white text-xs rounded px-2 py-1 border border-gray-600"
              defaultValue=""
              onChange={(e) => {
                const tpl = templates.find((t) => t.id === e.target.value);
                if (tpl) applyTemplate(tpl);
              }}
            >
              <option value="" disabled>Load template...</option>
              {templates.map((t) => (
                <option key={t.id} value={t.id}>
                  {t.name} ({t.difficulty}, {t.nodeCount} nodes)
                </option>
              ))}
            </select>
          )}
        </div>
        <div className="flex items-center gap-3">
          <button
            className="px-3 py-1.5 bg-gray-700 rounded text-sm hover:bg-gray-600"
            onClick={() => setShowConfigPanel(!showConfigPanel)}>
            {showConfigPanel ? 'Hide Config' : 'Config'}
          </button>
          <button
            className={`px-4 py-1.5 rounded text-sm font-medium ${
              isExecuting
                ? 'bg-gray-600 cursor-not-allowed'
                : 'bg-green-600 hover:bg-green-700'
            }`}
            disabled={isExecuting}
            onClick={handleExecute}
          >
            {isExecuting ? 'Executing...' : '▶ Execute'}
          </button>
          <button
            className="px-3 py-1.5 bg-gray-700 rounded text-sm hover:bg-gray-600"
            onClick={() => {
              setExecEvents([]);
              setShowExecPanel(false);
              setCanvasNodes(undefined);
              setCanvasEdges(undefined);
              setCanvasKey((k) => k + 1);
            }}
          >
            Reset
          </button>
        </div>
      </header>

      <div className="flex flex-1 overflow-hidden">
        <div className="flex-1 relative">
          <WorkflowCanvas
            key={canvasKey}
            initialNodes={canvasNodes}
            initialEdges={canvasEdges}
            onWorkflowChange={handleWorkflowChange}
            onNodeSelect={setSelectedNode}
          />
        </div>
        {showConfigPanel && (
          <ConfigPanel
            selectedNode={selectedNode}
            models={models}
            onModelsChange={loadModels}
          />
        )}
      </div>

      {showExecPanel && (
        <ExecutionPanel
          events={execEvents}
          isExecuting={isExecuting}
          onClose={() => {
            setShowExecPanel(false);
            setExecEvents([]);
          }}
        />
      )}
    </div>
  );
}
