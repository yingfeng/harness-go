'use client';

import { Handle, Position, NodeProps } from '@xyflow/react';
import { ReactNode } from 'react';

interface NodeData {
  label: string;
  nodeName?: string;
  model?: string;
  systemPrompt?: string;
  condition?: string;
  transformCode?: string;
  temperature?: number;
}

function BaseNode({ data, selected, children }: { data: NodeData; selected?: boolean; children: ReactNode }) {
  return (
    <div
      className={`px-4 py-2 rounded-lg border-2 shadow-md min-w-[150px] ${
        selected ? 'border-blue-500 ring-2 ring-blue-300' : 'border-gray-300'
      } bg-white`}
    >
      {children}
    </div>
  );
}

export function StartNode({ selected, data }: NodeProps) {
  const d = data as unknown as NodeData;
  return (
    <BaseNode data={d} selected={selected}>
      <Handle type="source" position={Position.Bottom} />
      <div className="text-center">
        <div className="text-green-600 font-bold text-sm">START</div>
        <div className="text-xs text-gray-500">{d.label}</div>
      </div>
    </BaseNode>
  );
}

export function EndNode({ selected, data }: NodeProps) {
  const d = data as unknown as NodeData;
  return (
    <BaseNode data={d} selected={selected}>
      <Handle type="target" position={Position.Top} />
      <div className="text-center">
        <div className="text-red-600 font-bold text-sm">END</div>
        <div className="text-xs text-gray-500">{d.label}</div>
      </div>
    </BaseNode>
  );
}

export function AgentNode({ selected, data }: NodeProps) {
  const d = data as unknown as NodeData;
  return (
    <BaseNode data={d} selected={selected}>
      <Handle type="target" position={Position.Top} />
      <Handle type="source" position={Position.Bottom} />
      <div className="text-center">
        <div className="text-blue-600 font-bold text-sm">Agent</div>
        <div className="text-xs font-medium">{d.nodeName || d.label}</div>
        {d.model && <div className="text-xs text-gray-400 mt-1">{d.model}</div>}
      </div>
    </BaseNode>
  );
}

export function IfElseNode({ selected, data }: NodeProps) {
  const d = data as unknown as NodeData;
  return (
    <BaseNode data={d} selected={selected}>
      <Handle type="target" position={Position.Top} />
      <Handle
        type="source"
        position={Position.Left}
        id="true"
        style={{ background: '#22c55e', width: 10, height: 10 }}
      />
      <Handle
        type="source"
        position={Position.Right}
        id="false"
        style={{ background: '#ef4444', width: 10, height: 10 }}
      />
      <div className="text-center">
        <div className="text-amber-600 font-bold text-sm">If/Else</div>
        <div className="text-xs font-medium">{d.nodeName || d.label}</div>
        {d.condition && (
          <div className="text-xs text-gray-400 mt-1 truncate max-w-[120px]">{d.condition}</div>
        )}
      </div>
    </BaseNode>
  );
}

export function WhileNode({ selected, data }: NodeProps) {
  const d = data as unknown as NodeData;
  return (
    <BaseNode data={d} selected={selected}>
      <Handle type="target" position={Position.Top} />
      <Handle type="source" position={Position.Bottom} id="continue" style={{ background: '#3b82f6', width: 10, height: 10 }} />
      <Handle type="source" position={Position.Left} id="break" style={{ background: '#f59e0b', width: 10, height: 10 }} />
      <div className="text-center">
        <div className="text-indigo-600 font-bold text-sm">While</div>
        <div className="text-xs font-medium">{d.nodeName || d.label}</div>
      </div>
    </BaseNode>
  );
}

export function UserApprovalNode({ selected, data }: NodeProps) {
  const d = data as unknown as NodeData;
  return (
    <BaseNode data={d} selected={selected}>
      <Handle type="target" position={Position.Top} />
      <Handle type="source" position={Position.Bottom} />
      <div className="text-center">
        <div className="text-pink-600 font-bold text-sm">User Approval</div>
        <div className="text-xs font-medium">{d.nodeName || d.label}</div>
      </div>
    </BaseNode>
  );
}

export function TransformNode({ selected, data }: NodeProps) {
  const d = data as unknown as NodeData;
  return (
    <BaseNode data={d} selected={selected}>
      <Handle type="target" position={Position.Top} />
      <Handle type="source" position={Position.Bottom} />
      <div className="text-center">
        <div className="text-purple-600 font-bold text-sm">Transform</div>
        <div className="text-xs font-medium">{d.nodeName || d.label}</div>
      </div>
    </BaseNode>
  );
}
