'use client';

import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  addEdge,
  Connection,
  Node,
  Edge,
  NodeTypes,
  ReactFlowProvider,
  useReactFlow,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { useCallback, useRef, useEffect, useMemo } from 'react';
import { WorkflowNode, WorkflowEdge } from '@/lib/api';
import { StartNode, EndNode, AgentNode, IfElseNode, WhileNode, UserApprovalNode, TransformNode } from './CustomNodes';

const stableNodeTypes: NodeTypes = {
  start: StartNode,
  end: EndNode,
  agent: AgentNode,
  'if-else': IfElseNode,
  while: WhileNode,
  'user-approval': UserApprovalNode,
  transform: TransformNode,
};

interface WorkflowCanvasProps {
  initialNodes?: Node[];
  initialEdges?: Edge[];
  onWorkflowChange: (nodes: WorkflowNode[], edges: WorkflowEdge[]) => void;
  onNodeSelect: (node: WorkflowNode | null) => void;
}

const defaultNodes: Node[] = [
  {
    id: 'start-1',
    type: 'start',
    position: { x: 250, y: 0 },
    data: { label: 'Start', nodeName: 'Start' },
  },
  {
    id: 'end-1',
    type: 'end',
    position: { x: 250, y: 500 },
    data: { label: 'End', nodeName: 'End' },
  },
];

function nodeToWF(n: Node): WorkflowNode {
  return {
    id: n.id,
    type: n.type || 'agent',
    position: { x: (n.position as any)?.x || 0, y: (n.position as any)?.y || 0 },
    data: n.data as any,
  };
}

function edgeToWF(e: Edge): WorkflowEdge {
  return {
    id: e.id,
    source: e.source,
    target: e.target,
    label: (e.label as string | undefined) ?? undefined,
    sourceHandle: (e.sourceHandle as string | undefined) ?? undefined,
  };
}

function CanvasInner({ initialNodes, initialEdges, onWorkflowChange, onNodeSelect }: WorkflowCanvasProps) {
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes || defaultNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges || []);
  const { screenToFlowPosition } = useReactFlow();
  const idCounter = useRef(initialNodes ? initialNodes.length + 1 : 3);

  // Sync internal node/edge state to parent whenever it changes.
  useEffect(() => {
    onWorkflowChange(nodes.map(nodeToWF), edges.map(edgeToWF));
  }, [nodes, edges, onWorkflowChange]);

  const onConnect = useCallback(
    (connection: Connection) => {
      setEdges((eds) => addEdge(connection, eds));
    },
    [setEdges]
  );

  const onNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      onNodeSelect(nodeToWF(node));
    },
    [onNodeSelect]
  );

  const addNode = useCallback(
    (type: string, label: string) => {
      const id = `node-${idCounter.current++}`;
      const position = screenToFlowPosition({ x: window.innerWidth / 2 - 75, y: 200 });
      const newNode: Node = {
        id,
        type,
        position,
        data: {
          label,
          nodeName: label,
          systemPrompt: type === 'agent' ? 'You are a helpful assistant.' : '',
          condition: type === 'if-else' ? '' : '',
        },
      };
      setNodes((nds) => [...nds, newNode]);
    },
    [screenToFlowPosition, setNodes]
  );

  const onPaneClick = useCallback(() => {
    onNodeSelect(null);
  }, [onNodeSelect]);

  return (
    <div className="w-full h-full relative">
      <div className="absolute top-2 left-2 z-10 flex gap-2">
        <button
          className="px-3 py-1.5 bg-blue-600 text-white text-sm rounded hover:bg-blue-700"
          onClick={() => addNode('agent', 'Agent')}
        >
          + Agent
        </button>
        <button
          className="px-3 py-1.5 bg-amber-600 text-white text-sm rounded hover:bg-amber-700"
          onClick={() => addNode('if-else', 'Condition')}
        >
          + If/Else
        </button>
        <button
          className="px-3 py-1.5 bg-purple-600 text-white text-sm rounded hover:bg-purple-700"
          onClick={() => addNode('transform', 'Transform')}
        >
          + Transform
        </button>
        <button
          className="px-3 py-1.5 bg-indigo-600 text-white text-sm rounded hover:bg-indigo-700"
          onClick={() => addNode('while', 'Loop')}
        >
          + While
        </button>
      </div>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeClick={onNodeClick}
        onPaneClick={onPaneClick}
        nodeTypes={stableNodeTypes}
        fitView
      >
        <Background />
        <Controls />
        <MiniMap />
      </ReactFlow>
    </div>
  );
}

export default function WorkflowCanvas(props: WorkflowCanvasProps) {
  return (
    <ReactFlowProvider>
      <CanvasInner {...props} />
    </ReactFlowProvider>
  );
}
